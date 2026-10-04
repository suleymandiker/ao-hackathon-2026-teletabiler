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
    """OpenAI-compatible kurumsal endpoint'e tek bir sade istek gönderir."""
    config = MODELS_CONFIG.get(agent_key)
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

    # Qwen yalnız RCA yorumu için kullanılır; kısa ve doğrudan JSON cevap isteriz.
    if config["family"] == "qwen":
        payload.setdefault("chat_template_kwargs", {})["enable_thinking"] = QWEN_RCA_ENABLE_THINKING
        ##payload.setdefault("include_reasoning", QWEN_RCA_INCLUDE_REASONING)

    headers = {
        "Authorization": f"Bearer {config['key']}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    print(
        f"[AI] CALL | agent={agent_key} | model={config['model_id']} | "
        f"endpoint={config['url']}"
    )

    start = time.time()
    try:
        session = requests.Session()
        if config["family"] == "qwen":
            session.trust_env = False
        #print("ANALİZ...")    
        #print(config["url"])
        #print(headers)
        #print(payload)
        #print("ANALİZ SONU...")   
        response = session.post(
            config["url"], headers=headers, json=payload, verify=False, timeout=(60, 180)
        )

        # Bazı gateway sürümleri response_format kabul etmiyor; bir kez sade tekrar et.
        if response.status_code == 400 and response_format is not None:
            payload.pop("response_format", None)
            response = session.post(
                config["url"], headers=headers, json=payload, verify=False, timeout=(60, 180)
            )

        duration = time.time() - start
        if response.status_code != 200:
            error = f"API Hatası ({response.status_code}): {response.text[:500]}"
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
        print(
            f"[AI] SUCCESS | agent={agent_key} | duration={duration:.2f}s | "
            f"tokens={usage_info['total_tokens']} | finish_reason={finish_reason}"
        )
        if return_usage:
            return reply, duration, usage_info
        return reply, duration

    except Exception as exc:
        duration = time.time() - start
        error = f"Bağlantı Hatası: {exc}"
        print(f"[AI] CONNECTION_ERROR | agent={agent_key} | duration={duration:.2f}s | {exc}")
        return _result(error, duration, return_usage)
