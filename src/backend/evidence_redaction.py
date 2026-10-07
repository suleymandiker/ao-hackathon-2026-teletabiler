"""Presentation-only credential redaction; never changes processing inputs."""
import re


SENSITIVE_KEY = re.compile(
    r'(?i)(?:authorization|proxy.authorization|.*api.?key|.*token|.*password|.*passwd|'
    r'.*secret|.*credential.*|cookie|set.cookie|x.auth.*|user.?id|private.?key)\Z')
_LABEL = (r'(?:authorization|proxy[-_]authorization|[\w-]*api[-_]?key|[\w-]*token|'
          r'[\w-]*password|passwd|[\w-]*secret|[\w-]*credentials?|cookie|set-cookie|x-auth[\w-]*|user[_-]?id)')
_ASSIGNMENT = re.compile(
    r'''(?i)\b''' + _LABEL + r'''["']?\s*[:=]\s*(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\r\n,;]+)''')


def redact_text(value, secrets=()):
    text = str(value)
    for secret in sorted((s for s in secrets if s), key=len, reverse=True):
        text = text.replace(secret, '[redacted]')
    text = re.sub(r'-----BEGIN [^-]*PRIVATE KEY-----[\s\S]*?(?:-----END [^-]*PRIVATE KEY-----|$)',
                  '[redacted]', text)
    text = _ASSIGNMENT.sub('[redacted]', text)
    text = re.sub(r'''(?i)\b(?:bearer|basic)\s+[^\s,;"']+''', '[redacted]', text)
    text = re.sub(r'\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b', '[redacted]', text)
    text = re.sub(r'\b(?:sk-[A-Za-z0-9_-]{12,}|AIza[A-Za-z0-9_-]{20,}|AKIA[A-Z0-9]{16})\b', '[redacted]', text)
    return re.sub(r'(https?://)[^\s/@:]+:[^\s/@]+@', r'\1[redacted]@', text)
