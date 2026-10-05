"""AOP için sade AI erişim katmanı.

Sadece iki görev ailesi vardır:
- DeepSeek: segmentasyon ve parser keşfi
- Qwen: olay/case analizi ve RCA uzman yorumu

Pipeline'ın sinyal, korelasyon ve olay üretimi deterministiktir; bu dosya o
kararları değiştirmez.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import requests
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

BACKEND_DIR = Path(__file__).resolve().parent
PROMPTS_DIR = BACKEND_DIR / "prompts"

DEEPSEEK_URL = os.getenv(
    "DEEPSEEK_URL",
    "https://common-inference-apis.turkcelltech.ai/glm-53-flash-dynamo-saka/v1/chat/completions",
)
DEEPSEEK_MODEL_ID = os.getenv("DEEPSEEK_MODEL_ID", "saka__glm-53-flash-dynamo-saka")
SAKA_API_KEY = os.getenv("SAKA_API_KEY", "")

QWEN_RCA_URL = os.getenv(
    "QWEN_RCA_URL",
    "https://common-inference-apis.turkcelltech.ai/glm-53-flash-dynamo-saka/v1/chat/completions",
)
QWEN_RCA_MODEL_ID = os.getenv(
    "QWEN_RCA_MODEL_ID", "saka__glm-53-flash-dynamo-saka"
)
QWEN_RCA_ENABLE_THINKING = os.getenv("QWEN_RCA_ENABLE_THINKING", "false").lower() == "true"
QWEN_RCA_INCLUDE_REASONING = os.getenv("QWEN_RCA_INCLUDE_REASONING", "false").lower() == "true"

# Kodun geri kalanının kullandığı üç sabit rol. Başka provider/alias yok.
MODELS_CONFIG = {
    "Segmentation_Discovery": {
        "url": DEEPSEEK_URL,
        "key": SAKA_API_KEY,
        "model_id": DEEPSEEK_MODEL_ID,
        "family": "deepseek",
    },
    "Parser_Discovery": {
        "url": DEEPSEEK_URL,
        "key": SAKA_API_KEY,
        "model_id": DEEPSEEK_MODEL_ID,
        "family": "deepseek",
    },
    "Ajan_2_RCA_Expert": {
        "url": QWEN_RCA_URL,
        "key": SAKA_API_KEY,
        "model_id": QWEN_RCA_MODEL_ID,
        "family": "qwen",
        # Observed capabilities of this configured RCA deployment, not its family.
        "capabilities": {
            "supports_enable_thinking": True,
            "supports_include_reasoning": False,
            "supports_json_object": True,
            "supports_json_schema": True,
        },
    },
}


def load_prompt(prompt_filename: str, **variables: Any) -> str:
    """Prompt dosyasını yükler ve basit {{degisken}} alanlarını doldurur."""
    prompt_path = PROMPTS_DIR / prompt_filename
    try:
        rendered = prompt_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise FileNotFoundError(
            f"Prompt dosyası okunamadı: {prompt_filename}: {exc}"
        ) from exc

    if "{{common_system}}" in rendered:
        common = (PROMPTS_DIR / "common_system.md").read_text(encoding="utf-8").strip()
        rendered = rendered.replace("{{common_system}}", common)

    for key, value in variables.items():
        rendered = rendered.replace("{{" + str(key) + "}}", str(value))
    return rendered


def _result(error: str, duration: float, return_usage: bool):
    if return_usage:
        return error, duration, {}
    return error, duration


def safe_usage(usage):
    """Allowlisted gateway measurements; missing tokens stay unknown, not estimated."""
    usage = usage if isinstance(usage, dict) else {}
    result = {key: value if type(value := usage.get(key)) is int and value >= 0 else None
              for key in ('prompt_tokens', 'completion_tokens', 'total_tokens')}
    reason = usage.get('finish_reason')
    result['finish_reason'] = reason if reason in ('stop', 'length', 'max_tokens', 'content_filter', 'tool_calls') else 'unknown'
    return result


def _apply_rca_capabilities(payload, capabilities):
    """Filter optional fields after extra_body; never mutate caller-owned options."""
    template_kwargs = dict(payload.get('chat_template_kwargs') or {})
    if capabilities.get('supports_enable_thinking', False):
        template_kwargs['enable_thinking'] = QWEN_RCA_ENABLE_THINKING
    else:
        template_kwargs.pop('enable_thinking', None)
    if template_kwargs:
        payload['chat_template_kwargs'] = template_kwargs
    else:
        payload.pop('chat_template_kwargs', None)
    if capabilities.get('supports_include_reasoning', False):
        payload['include_reasoning'] = QWEN_RCA_INCLUDE_REASONING
    else:
        payload.pop('include_reasoning', None)

    requested = payload.get('response_format')
    mode = requested.get('type') if isinstance(requested, dict) else None
    if mode == 'json_schema' and not capabilities.get('supports_json_schema', False):
        mode = 'json_object'
        payload['response_format'] = {'type': mode}
    if mode not in ('json_schema', 'json_object') or not capabilities.get('supports_' + mode, False):
        payload.pop('response_format', None)


def _rca_error_diagnostics(response):
    """Only emit known identifiers; gateway messages and validation inputs may echo secrets."""
    result = dict.fromkeys(('error_type', 'error_code', 'rejected_field', 'schema_keyword'), 'unknown')
    try:
        body = response.json()
    except (ValueError, RecursionError):
        body = response.text
    error = body.get('error', body.get('detail', body)) if isinstance(body, dict) else body
    if isinstance(error, list):
        error = error[0] if error else {}
    message = error.get('message') if isinstance(error, dict) else error
    if not isinstance(error, dict):
        error = {}
    allowed = {
        'type': ('invalid_request_error', 'BadRequestError', 'bad_request', 'validation_error',
                 'value_error', 'extra_forbidden', 'unsupported_parameter', 'server_error',
                 'authentication_error', 'permission_error', 'rate_limit_error'),
        'code': ('invalid_json_schema', 'invalid_schema', 'unsupported_schema',
                 'unsupported_response_format', 'unsupported_parameter', 'unsupported_value',
                 'invalid_parameter', 'invalid_request_error', 'extra_forbidden',
                 'context_length_exceeded', 'model_not_found', 'invalid_api_key'),
    }
    for key, values in allowed.items():
        value = error.get(key)
        if isinstance(value, str) and value in values:
            result['error_' + key] = value

    path = error.get('param')
    if isinstance(path, str):
        path = path.split('.')
    else:
        path = error.get('loc', [])
    if not isinstance(path, (list, tuple)):
        return result
    if path and path[0] == 'body':
        path = path[1:]
    fields = ('response_format', 'model', 'max_tokens', 'temperature', 'messages',
              'chat_template_kwargs', 'enable_thinking', 'include_reasoning')
    # Recognize the observed gateway wording by exact match only. Never log the
    # message itself or extract arbitrary values from it (it may echo evidence).
    for field in fields + ('json_schema', 'json_object'):
        if message == f'Validation: Unsupported parameter(s): `{field}`':
            result['error_code'] = 'unsupported_parameter'
            result['rejected_field'] = 'response_format' if field in ('json_schema', 'json_object') else field
            break
    if path and isinstance(path[0], str) and path[0] in fields:
        result['rejected_field'] = path[0]
    keywords = ('type', 'properties', 'required', 'additionalProperties', 'minLength',
                'maxLength', 'minimum', 'maximum', 'enum', 'items', 'minItems', 'maxItems',
                'nullable', '$schema', '$ref', 'anyOf', 'allOf', 'oneOf')
    if (result['rejected_field'] == 'response_format' and 'schema' in path
            and isinstance(path[-1], str) and path[-1] in keywords):
        result['schema_keyword'] = path[-1]
    return result


def _debug_rca_request(payload):
    """Opt-in console inspection of final messages and allowlisted request fields."""
    if os.getenv('RCA_DEBUG', 'false').strip().lower() not in ('1', 'true', 'yes', 'on'):
        return
    messages = payload['messages']
    system_prompt = next(message['content'] for message in messages if message['role'] == 'system')
    user_content = next(message['content'] for message in messages if message['role'] == 'user')
    mode = payload.get('response_format', {}).get('type', 'none')
    thinking = payload.get('chat_template_kwargs', {}).get('enable_thinking', 'absent')
    try:
        print(
            f'[RCA DEBUG] SYSTEM PROMPT\n{system_prompt}\n\n'
            f'[RCA DEBUG] EVIDENCE PACK\n{user_content}\n\n'
            '[RCA DEBUG] REQUEST CONFIG\n'
            f'model={payload.get("model", "absent")}\n'
            f'temperature={payload.get("temperature", "absent")}\n'
            f'max_tokens={payload.get("max_tokens", "absent")}\n'
            f'response_mode={mode}\n'
            f'enable_thinking={thinking}\n'
            f'include_reasoning={payload.get("include_reasoning", "absent")}',
            flush=True,
        )
    except (OSError, UnicodeError):
        # Local/Citrix console failures must not prevent the RCA request.
        pass


def _post_rca(session, url, headers, payload):
    """One no-format retry only for an explicit unsupported-format HTTP 400."""
    initial = payload.get('response_format')
    formats = [initial, None] if initial is not None else [None]
    for attempt, format_value in enumerate(formats, 1):
        if format_value is None:
            payload.pop('response_format', None)
            mode = 'none'
        else:
            payload['response_format'] = format_value
            mode = format_value.get('type') if isinstance(format_value, dict) else None
            if mode not in ('json_schema', 'json_object'):
                mode = 'other_response_format'
        detail = f'agent=Ajan_2_RCA_Expert | attempt={attempt} | response_mode={mode}'
        if attempt > 1:
            print(f'[AI] RETRY | {detail}')
        print(f'[AI] ATTEMPT | {detail}')
        _debug_rca_request(payload)
        response = session.post(url, headers=headers, json=payload, verify=False, timeout=(60, 180))
        diagnostics = _rca_error_diagnostics(response) if response.status_code != 200 else {}
        suffix = ''.join(f' | {key}={value}' for key, value in diagnostics.items())
        print(f'[AI] HTTP_{response.status_code} | {detail}{suffix}')
        if response.status_code != 400:
            break
        field, code = diagnostics['rejected_field'], diagnostics['error_code']
        unsupported_format = (
            field in ('unknown', 'response_format') and code in ('unsupported_response_format', 'unsupported_schema')
            or field == 'response_format' and code in ('unsupported_parameter', 'unsupported_value')
        )
        if not unsupported_format:
            break
    return response


def call_ai_agent(
    agent_key: str,
    system_prompt: str,
    user_content: str,
    temperature: float = 0.1,
    max_tokens: int | None = None,
    response_format: dict | None = None,
    return_usage: bool = False,
    extra_body: dict | None = None,
):
    """Call the OpenAI-compatible gateway, with bounded response-format retries."""
    config = MODELS_CONFIG.get(agent_key)
    is_rca = agent_key == 'Ajan_2_RCA_Expert'
    if config is None:
        return _result(f"Hata: {agent_key} konfigürasyonu bulunamadı.", 0.0, return_usage)

    if not config["key"]:
        return _result(f"Hata: {agent_key} için API anahtarı tanımlı değil.", 0.0, return_usage)

    payload = {
        "model": config["model_id"],
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": temperature,
    }
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens
    if response_format is not None:
        payload["response_format"] = response_format
    if extra_body:
        payload.update(extra_body)

    if is_rca:
        _apply_rca_capabilities(payload, config.get('capabilities', {}))
    elif config["family"] == "qwen":
        payload.setdefault("chat_template_kwargs", {})["enable_thinking"] = QWEN_RCA_ENABLE_THINKING
        payload["include_reasoning"] = QWEN_RCA_INCLUDE_REASONING

    headers = {
        "Authorization": f"Bearer {config['key']}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    if is_rca:
        print('[AI] CALL | agent=Ajan_2_RCA_Expert')
    else:
        print(
            f"[AI] CALL | agent={agent_key} | model={config['model_id']} | "
            f"endpoint={config['url']}"
        )

    start = time.time()
    session = None
    try:
        session = requests.Session()
        if config["family"] == "qwen":
            session.trust_env = False
        if is_rca:
            response = _post_rca(session, config['url'], headers, payload)
        else:
            response = session.post(
                config["url"], headers=headers, json=payload, verify=False, timeout=(60, 180)
            )
            # Preserve discovery agents' existing one-time format fallback.
            if response.status_code == 400 and response_format is not None:
                payload.pop("response_format", None)
                response = session.post(
                    config["url"], headers=headers, json=payload, verify=False, timeout=(60, 180)
                )

        duration = time.time() - start
        if response.status_code != 200:
            error = (f'RCA API error ({response.status_code})' if is_rca
                     else f"API Hatası ({response.status_code}): {response.text[:500]}")
            print(f"[AI] FAIL | agent={agent_key} | duration={duration:.2f}s | {error}")
            return _result(error, duration, return_usage)

        body = response.json()
        reply = body.get("choices", [{}])[0].get("message", {}).get("content") or ""
        usage = body.get("usage", {}) or {}
        finish_reason = body.get("choices", [{}])[0].get("finish_reason")
        usage_info = {
            "prompt_tokens": usage.get("prompt_tokens", 0) or 0,
            "completion_tokens": usage.get("completion_tokens", 0) or 0,
            "total_tokens": usage.get("total_tokens", 0) or 0,
            "finish_reason": finish_reason,
        }
        if is_rca:
            usage_info = safe_usage({**usage, 'finish_reason': finish_reason})
            finish_reason = usage_info['finish_reason']
        print(
            f"[AI] SUCCESS | agent={agent_key} | duration={duration:.2f}s | "
            f"tokens={usage_info['total_tokens']} | finish_reason={finish_reason}"
        )
        if return_usage:
            return reply, duration, usage_info
        return reply, duration

    except Exception as exc:
        duration = time.time() - start
        error = 'RCA connection error' if is_rca else f"Bağlantı Hatası: {exc}"
        print(f"[AI] CONNECTION_ERROR | agent={agent_key} | duration={duration:.2f}s | {error}")
        return _result(error, duration, return_usage)
    finally:
        if session is not None:
            session.close()
