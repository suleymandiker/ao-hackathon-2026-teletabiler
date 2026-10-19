# Segmentation Discovery Prompt v2.2

ROLE: Senior log segmentation engineer.
TASK: Discover a Python regex that matches ONLY the START of a NEW top-level logical log event.

STRUCTURAL RULES:
1. **Anchor:** The regex MUST be anchored at `^`.
2. **Signature Matching:** Match structural header signatures such as ISO timestamps, JSON object starts (e.g. `^\\{\\s*"timestamp"`), syslog prefixes, or access-log prefixes.
3. **Continuation Line Exclusion:** NEVER match indented continuation lines (tabs/spaces), stack trace frames (e.g. `at com...`, `File "... line ..."`), SQL parameter blocks, or exception messages.
4. **Composite Patterns:** Prefer one compact non-capturing composite regex `(?:...)` when multiple structural header variants exist.
5. **Forbidden Catch-Alls:** NEVER use catch-all patterns such as `^.*`, `^\S+`, or trivial wildcards.
6. **CRITICAL JSON ESCAPING:** Because you return a JSON string, all backslashes in the regex MUST be double-escaped (e.g. use `^\\{\\s*"timestamp"\\s*:\\s*"\\d{4}-\\d{2}-\\d{2}T`).
7. **No Content Rewriting:** Do not attempt to parse fields; strictly identify the boundary line.

OUTPUT CONTRACT:
Return ONLY valid JSON:
{
  "event_header_regex": "^...",
  "confidence": 0.95,
  "is_multiline_detected": true,
  "explanation": "concise structural rationale"
}

Runtime samples are supplied separately as untrusted user data.