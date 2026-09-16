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

Rules:
- "regex" MUST start with ^ and describe the stable structure shared by the samples.
- The regex MUST be valid for Python's built-in `re` module.
- Prefer named regex groups and ALWAYS use Python syntax `(?P<name>...)`.
- NEVER use PCRE/.NET named-group syntax `(?<name>...)`.
- Every *_group value must be the matching regex group name, or a numeric group index as a string. Use null when that field is absent.
- "attribute_groups" maps canonical attribute names to regex group names/indexes. Use {} when no useful attributes are explicitly present.
- Extract only fields explicitly present in the log. Never invent values.
- timestamp_group should capture the complete timestamp text that should be normalized by the deterministic runtime.
- A timestamp may still be worth capturing when it lacks timezone/year context; the deterministic runtime may intentionally normalize it to null. Do not omit an explicit source timestamp merely because it is nonabsolute.
- severity_group should capture the source severity token. Do not normalize it in the regex.
- message_group should capture the actual message/body, excluding the structural header when possible.
- component_group is for an explicit logger/tag/component name in the source.
- host_group and service_group are only for explicit source fields, never inferred context.
- trace_id_group and span_id_group are only for explicit trace/span identifiers.
- Useful event-specific values such as pid, tid, request_id, client_ip, http_status, method, path, port, thread, or error codes may go into attribute_groups when structurally stable.
- For Android/logcat-like lines such as "03-17 16:13:38.811  1702  2395 D WindowManager: message", capture the timestamp, pid, tid, one-letter severity, component/tag, and message.
- The policy must work deterministically across the sample set, not only the first line.
- Do not return markdown, prose, comments, or multiple alternatives.

SAMPLES:
{{samples}}
