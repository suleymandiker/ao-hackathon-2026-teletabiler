import re
from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer


class StructuredTextParser:
    HEALTHAPP_PIPE_HEADER = re.compile(
        r'^(?P<timestamp>\d{8}-\d{1,2}:\d{1,2}:\d{1,2}:\d{1,3})\|'
        r'(?P<component>[^|\r\n]+)\|'
        r'(?P<opaque_numeric>\d+)\|'
        r'(?P<message>.*)$'
    )
    WINDOWS_CBS_HEADER = re.compile(
        r'^(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}),\s*'
        r'(?P<severity>FATAL|CRITICAL|ERROR|WARNING|WARN|NOTICE|INFO|DEBUG|TRACE)'
        r'\s{2,}(?P<component>CBS|CSI)\s{2,}(?P<message>.*)$',
        re.I,
    )
    ZOOKEEPER_HEADER = re.compile(
        r'^(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})\s+-\s+'
        r'(?P<severity>FATAL|CRITICAL|ERROR|WARNING|WARN|NOTICE|INFO|DEBUG|TRACE)\s+'
        r'\[(?P<context>.*:)(?P<component>[A-Za-z_$][\w.$-]*)@(?P<line>\d+)\]'
        r'\s+-\s+(?P<message>.*)$',
        re.I,
    )
    SEVERITY_PATTERN = re.compile(
        r'(?<![A-Za-z])(?:FATAL|CRITICAL|ERROR|WARNING|WARN|NOTICE|INFO|DEBUG|TRACE)(?![A-Za-z])',
        re.I,
    )

    # High-confidence producer boundary after a recognized timestamp:
    #   [timestamp] process.exe - semantic message
    #
    # Keep this deliberately narrow. Generic "foo - bar" text is not enough
    # evidence to strip "foo"; executable suffixes are strong producer evidence.
    EXECUTABLE_PRODUCER = re.compile(
        r'^(?P<component>[A-Za-z0-9_.-]+\.(?:exe|com|bat|cmd|bin))\s+-\s+'
        r'(?P<message>\S.*)$',
        re.I,
    )

    # Strong, format-structural extractors. These intentionally prefer precision
    # over recall: optional canonical fields are populated only when their role is
    # unambiguous in the header.
    COMPONENT_AFTER_SEVERITY = re.compile(
        r'^\s*(?:\d+\s+)?'
        r'([A-Za-z_$][\w.$-]*(?:\.[A-Za-z_$][\w.$-]*)+)\s*(?:\[|:)',
        re.I,
    )
    COMPONENT_AFTER_THREAD = re.compile(
        r'^\s*\[[^\]]+\]\s+'
        r'([A-Za-z_$][\w.$-]*(?:\.[A-Za-z_$][\w.$-]*)+)\s*:',
        re.I,
    )
    # Strong application-service evidence only. A lowercase hyphenated token in
    # the leading bracket is treated as a service; generic thread labels such as
    # [main] are deliberately excluded. An unbracketed token must end in
    # "-service" and be followed by an explicit key=value field.
    BRACKETED_SERVICE = re.compile(
        r'^\[(?P<service>[a-z][a-z0-9]*(?:-[a-z0-9]+)+)\](?=\s|$)'
    )
    BARE_SERVICE = re.compile(
        r'^(?P<service>[a-z][a-z0-9-]*-service)\s+'
        r'(?=[A-Za-z_][A-Za-z0-9_.-]*=)'
    )
    REQUEST_ID_PATTERN = re.compile(r'\b(req-[0-9a-fA-F-]{8,})\b')
    HTTP_STATUS_PATTERN = re.compile(r'\bstatus\s*[:=]\s*(\d{3})\b', re.I)
    HTTP_REQUEST_PATTERN = re.compile(
        r'"(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+[^"\r\n]*"', re.I
    )
    CLIENT_IP_BEFORE_HTTP = re.compile(
        r'(?:^|\]\s+|\s)'
        r'((?:\d{1,3}\.){3}\d{1,3})'
        r'(?:,(?:\d{1,3}\.){3}\d{1,3})?\s+'
        r'(?="(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s)',
        re.I,
    )

    # Conservative explicit scalar key=value extraction for structured text.
    # Complex/container values are intentionally excluded here; they need a
    # format-aware parser rather than a partial generic capture.
    EXPLICIT_SCALAR_KV = re.compile(
        r'(?<![\w.-])(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)='
        r'(?P<value>"[^"\r\n]*"|\'[^\'\r\n]*\'|[^\s,()\[\]{}]+)'
    )

    def __init__(self):
        self.ts = TimestampNormalizer()
        self.nginx_regex = re.compile(
            r'^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(.*?)"\s+(\d{3})\s+(\d+)'
        )

    @staticmethod
    def _normalize_severity(value):
        if not value:
            return None
        value = value.upper()
        return "WARNING" if value == "WARN" else value

    @staticmethod
    def _valid_ipv4(value):
        if not value:
            return None
        try:
            octets = value.split(".")
            if len(octets) == 4 and all(0 <= int(part) <= 255 for part in octets):
                return value
        except (TypeError, ValueError):
            pass
        return None

    @staticmethod
    def _convert_scalar_attribute(value):
        clean = value
        if (
            len(clean) >= 2
            and clean[0] == clean[-1]
            and clean[0] in ('"', "'")
        ):
            clean = clean[1:-1]

        if re.fullmatch(r"[+-]?\d+", clean):
            unsigned = clean.lstrip("+-")
            if len(unsigned) > 1 and unsigned.startswith("0"):
                return clean
            try:
                return int(clean)
            except ValueError:
                return clean

        try:
            return float(clean)
        except ValueError:
            return clean

    def _extract_scalar_attributes(self, first_line):
        attributes = {}
        for match in self.EXPLICIT_SCALAR_KV.finditer(first_line):
            key = match.group("key")
            value = match.group("value")
            if key not in attributes:
                attributes[key] = self._convert_scalar_attribute(value)
        return attributes

    def _extract_optional_fields(self, first_line, header_after_severity):
        fields = {}
        attributes = self._extract_scalar_attributes(first_line)

        service_match = (
            self.BRACKETED_SERVICE.match(header_after_severity)
            or self.BARE_SERVICE.match(header_after_severity)
        )
        if service_match:
            fields["service"] = service_match.group("service")

        component_match = (
            self.COMPONENT_AFTER_THREAD.match(header_after_severity)
            or self.COMPONENT_AFTER_SEVERITY.match(header_after_severity)
        )
        if component_match:
            fields["component"] = component_match.group(1)

        request_match = self.REQUEST_ID_PATTERN.search(first_line)
        if request_match:
            attributes["request_id"] = request_match.group(1)

        # Network fields are extracted only from an explicit HTTP request context.
        # This avoids treating arbitrary IP addresses in log prose as client_ip.
        if self.HTTP_REQUEST_PATTERN.search(first_line):
            ip_match = self.CLIENT_IP_BEFORE_HTTP.search(first_line)
            if ip_match:
                client_ip = self._valid_ipv4(ip_match.group(1))
                if client_ip:
                    fields["client_ip"] = client_ip

            status_match = self.HTTP_STATUS_PATTERN.search(first_line)
            if status_match:
                fields["http_status"] = int(status_match.group(1))

        fields["attributes"] = attributes
        return fields

    def parse(self, log):

        # Windows CBS.log / CSI rows expose the producer component as a stable
        # structural column after severity. Promote only the unambiguous CBS/CSI
        # token and remove it from message; timestamp remains nonabsolute because
        # the source does not provide timezone information.
        first_line = log.splitlines()[0] if log else ""
        windows_match = self.WINDOWS_CBS_HEADER.match(first_line)
        if windows_match:
            gd = windows_match.groupdict()
            return {
                "timestamp": self.ts.normalize(gd["timestamp"]),
                "severity": self._normalize_severity(gd["severity"]),
                "message": gd["message"],
                "component": gd["component"].upper(),
                "attributes": self._extract_scalar_attributes(gd["message"]),
            }

        # Zookeeper: final Class/Component@line token in the bracketed header is
        # structural producer evidence. The preceding bracket content is context,
        # not the component itself.
        zookeeper_match = self.ZOOKEEPER_HEADER.match(first_line)
        if zookeeper_match:
            gd = zookeeper_match.groupdict()

            # Explicit key=value pairs may live in either the structural header
            # context (for example myid=1) or the message. Preserve both without
            # inventing fields. Message values win only when the same explicit key
            # is also present in context; source-location line remains structural.
            attributes = self._extract_scalar_attributes(gd["context"])
            for key, value in self._extract_scalar_attributes(gd["message"]).items():
                attributes[key] = value
            attributes.setdefault("line", int(gd["line"]))

            return {
                "timestamp": self.ts.normalize(gd["timestamp"]),
                "severity": self._normalize_severity(gd["severity"]),
                "message": gd["message"],
                "component": gd["component"],
                "attributes": attributes,
            }

        if not log or not log.strip():
            return None

        # Preserve the logical event content. Extraction is based on the first
        # header line; continuation lines remain available in message/raw.
        event = log.strip()
        first_line = event.splitlines()[0]

        # Strong pipe-delimited application header:
        # timestamp | component | opaque numeric column | message.
        # The numeric column's business/technical meaning is not evidenced by
        # the raw format, so do not invent pid/tid/user/device semantics.
        healthapp_match = self.HEALTHAPP_PIPE_HEADER.match(first_line)
        if healthapp_match:
            groups = healthapp_match.groupdict()
            raw_timestamp = groups["timestamp"]
            return {
                "timestamp": self.ts.normalize(raw_timestamp),
                "severity": None,
                "message": groups["message"],
                "component": groups["component"],
                "attributes": {},
            }

        match = self.nginx_regex.match(first_line)
        if match:
            ip, timestamp_raw, request, status, size = match.groups()
            status_code = int(status)
            size_bytes = int(size) if size.isdigit() else 0

            if status_code >= 500:
                severity = "ERROR"
            elif status_code >= 400:
                severity = "WARNING"
            else:
                severity = "INFO"

            return {
                "timestamp": self.ts.normalize(timestamp_raw),
                "severity": severity,
                "message": f"{request.strip()} (HTTP {status_code})",
                "host": ip,
                "client_ip": ip,
                "http_status": status_code,
                "attributes": {
                    "http_status_code": status_code,
                    "response_size_bytes": size_bytes,
                },
            }

        timestamp, raw_timestamp = self.ts.extract(first_line)
        # A recognized source timestamp may be structurally useful even when it
        # cannot be normalized to an absolute CanonicalEvent timestamp. Example:
        # Apache "[Sun Dec 04 04:47:44 2005]" has no timezone. In that case
        # timestamp remains None, but we still parse severity/message instead of
        # falling back and losing the known header structure.
        if raw_timestamp is None:
            return None

        # Severity is searched only in the bounded header region after the
        # timestamp. This prevents stacktrace/prose words from overriding the
        # actual event level while supporting:
        #   TIMESTAMP ERROR ...
        #   PREFIX TIMESTAMP PID INFO ...
        ts_start = first_line.find(raw_timestamp)
        ts_end = ts_start + len(raw_timestamp)

        # TimestampNormalizer returns the timestamp value itself, not surrounding
        # structural delimiters. Consume a directly adjacent closing bracket so
        # Apache-style "[timestamp]" does not leak "]" into the message/header.
        header_start = ts_end
        if header_start < len(first_line) and first_line[header_start] == "]":
            header_start += 1

        header_tail = first_line[header_start:].lstrip()
        sev_match = self.SEVERITY_PATTERN.search(header_tail[:256])
        severity = self._normalize_severity(sev_match.group(0)) if sev_match else None

        # Optional field extraction starts after the actual severity token, so
        # PIDs/thread names cannot be confused with component/logger names.
        if sev_match:
            after_severity = header_tail[sev_match.end():].lstrip()
            # Apache error logs wrap severity as "[notice]". Consume only the
            # structural closing bracket adjacent to the recognized severity.
            if after_severity.startswith("]"):
                after_severity = after_severity[1:].lstrip()
        else:
            after_severity = header_tail
        optional_fields = self._extract_optional_fields(first_line, after_severity)

        # When no severity exists, a single executable producer followed by the
        # conventional " - " separator is strong structural evidence. Promote the
        # producer to component and keep only the semantic body as message.
        #
        # Do not apply this after an explicit severity: those formats already have
        # their own component/logger extraction rules and a generic dash in prose
        # must never become a message boundary.
        producer_match = None
        if not sev_match:
            producer_match = self.EXECUTABLE_PRODUCER.match(after_severity)
            if producer_match:
                optional_fields.setdefault("component", producer_match.group("component"))

        # Canonical message excludes recognized timestamp/severity header tokens.
        # RAW remains the exact original LogicalEvent in CanonicalEventBuilder.
        if producer_match:
            first_message = producer_match.group("message").strip()
        else:
            first_message = after_severity.strip() if sev_match else header_tail.strip()
        remaining = event.splitlines()[1:]
        message = "\n".join([first_message, *remaining]).strip() if remaining else first_message
        if not message:
            message = event

        return {
            "timestamp": timestamp,
            "severity": severity,
            "message": message,
            **optional_fields,
        }
