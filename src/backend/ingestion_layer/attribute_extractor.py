# ingestion_layer/attribute_extractor.py
from __future__ import annotations

import json
from typing import Any, Dict


# OpenSearch sorgusunda istenecek genişletilmiş alan projeksiyonu
OPENSEARCH_ENRICHED_SOURCE_FIELDS = [
    "timestamp",
    "@timestamp",
    "message",
    "level",
    "hostname",
    "openshift.sequence",
    "tcmonit_retag_tcserviceid",
    "kubernetes.pod_name",
    "kubernetes.pod_ip",
    "kubernetes.container_name",
    "kubernetes.container_image",
    "kubernetes.container_iostream",
    "kubernetes.labels.pod-template-hash",
    "kubernetes.namespace_labels.tcserviceid",
]


def extract_opensearch_attributes(hit_source: Dict[str, Any]) -> Dict[str, Any]:
    """OpenSearch _source belgesini ayrıştırarak pipeline attributes sözlüğüne dönüştürür.
    
    Nested JSON mesajlarını güvenle parse eder; boş/None değerleri temizler.
    """
    k8s = hit_source.get("kubernetes") or {}
    labels = k8s.get("labels") or {}
    ns_labels = k8s.get("namespace_labels") or {}
    openshift = hit_source.get("openshift") or {}

    # 1. Altyapı ve Konteyner Metadataları (P1 & P2 Fazları)
    attrs: Dict[str, Any] = {
        "hostname": hit_source.get("hostname"),
        "log_level": hit_source.get("level"),
        "sequence": openshift.get("sequence"),
        "service_id": hit_source.get("tcmonit_retag_tcserviceid") or ns_labels.get("tcserviceid"),
        "pod_name": k8s.get("pod_name"),
        "pod_ip": k8s.get("pod_ip"),
        "container_name": k8s.get("container_name"),
        "container_image": k8s.get("container_image"),
        "container_iostream": k8s.get("container_iostream"),
        "pod_template_hash": labels.get("pod-template-hash"),
    }

    # 2. Gömülü JSON Mesaj Analizi (P0 Anomaly & P3 Predictive Incident)
    raw_message = hit_source.get("message")
    if isinstance(raw_message, str):
        stripped = raw_message.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                msg_json = json.loads(stripped)
                if isinstance(msg_json, dict):
                    if "status_code" in msg_json:
                        attrs["http_status"] = msg_json.get("status_code")
                    if "process_time_ms" in msg_json:
                        attrs["process_time_ms"] = float(msg_json["process_time_ms"])
                    if "request_id" in msg_json:
                        attrs["request_id"] = str(msg_json["request_id"])
                    if "method" in msg_json:
                        attrs["http_method"] = str(msg_json["method"]).upper()
                    if "url" in msg_json:
                        attrs["http_url"] = str(msg_json["url"])
                    if "logger" in msg_json:
                        attrs["logger"] = str(msg_json["logger"])
            except (ValueError, TypeError):
                # Bozuk/yarım JSON'larda ham mesaj bütünlüğünü bozmadan devam et
                pass

    return {k: v for k, v in attrs.items() if v not in (None, "")}