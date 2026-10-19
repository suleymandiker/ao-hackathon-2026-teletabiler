Learn ONE deterministic parser policy for the unknown/custom log format represented by the bounded samples below.

Return ONLY one valid JSON object with exactly these policy fields:
{
  "regex": "...",
  "timestamp_group": null,
  "severity_group": null,
  "message_group": null,
  "host_group": null,
  "service_group": null,
  "component_group": null,
  "trace_id_group": null,
  "span_id_group": null,
  "attribute_groups": {}
}

RULES:
1. **Anchor & Scope:** "regex" MUST start with `^` and match the stable single-line header of each event across ALL samples.
2. **Python `re` Compatibility:** The regex MUST be fully compilable with Python's built-in `re` module.
3. **CRITICAL JSON ESCAPING:** All regex backslashes MUST be double-escaped in the JSON string (e.g. use `"\\\\d{4}"`, `"\\\\s+"`, `"\\\\b"`). Single unescaped backslashes like `\d` are INVALID JSON.
4. **Group Syntax:** Prefer named groups using strict Python syntax `(?P<name>...)`. Group names must be valid Python identifiers (alphanumeric and underscore only, NO hyphens, cannot start with a digit). NEVER use PCRE syntax `(?<name>...)`.
5. **Group Mappings:** Every `*_group` value must be the matching regex group name as a string, or `null` if that field does not exist in the log structure.
6. **Field Boundaries:**
   - `timestamp_group`: Capture the raw timestamp string to be normalized by runtime.
   - `severity_group`: Capture the raw severity token (ERROR, WARN, INFO, D, etc.). Do not normalize in regex.
   - `message_group`: Capture the primary message body, excluding headers.
   - `component_group`: Capture explicit logger/tag/class/component tokens.
   - `host_group` / `service_group`: Capture ONLY explicit source fields, never inferred context.
   - `attribute_groups`: Map extra stable attributes (e.g. `{"http_status": "status_code", "pid": "process_id"}`). Use `{}` if none exist.
7. **JSON Log Handling:** If samples are JSON objects, match the outer structure safely or capture top-level fields reliably.
8. **No Prose:** Return ONLY the JSON object. Do not wrap in markdown fences or include explanations.

SAMPLES:
{{samples}}