import sys
import os
from datetime import datetime, timedelta, timezone
import argparse
from pathlib import Path
import json
from dataclasses import dataclass

# 1. PATH AYARLAMALARI
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src" / "backend"))
sys.path.insert(0, str(ROOT / "src"))

from application_environment import load_environment
from opensearch_connection import load_opensearch_connection
from ingestion_layer.opensearch_client import OpenSearchClient
from ingestion_layer.opensearch_source import OpenSearchSource
from full_pipeline_v2 import FullAIOpsPipelineV2
from segmentation_layer.contracts import AssembledEvent
from parser_layer.timestamp.source_policy import TimestampSourcePolicy
from rca_layer.rca_engine import ExpertRCAEngine

# AssembledEvent katı tip doğrulamasını kapatıyoruz
AssembledEvent.__post_init__ = lambda self: None

# Global log havuzu (RCA zenginleştirmesi için)
ACTIVE_METADATA_LIST = []

# =========================================================================
# RCA MOTORU ENJEKTÖRÜ (TOPOLOJİ & POD METRİKLERİNİ SİNYALLERE BAĞLAR)
# =========================================================================
_orig_expert_analyze = ExpertRCAEngine.analyze

def patched_expert_analyze(self, incidents, correlations, signals=(), *args, **kwargs):
    signals_list = list(signals)
    incidents_list = list(incidents)

    # OpenSearch'ten toplanan pod ve topoloji verilerini sinyallere zerk ediyoruz
    if ACTIVE_METADATA_LIST:
        unique_pods = list({m.get('pod_name') for m in ACTIVE_METADATA_LIST if m.get('pod_name')})
        unique_ips = list({m.get('pod_ip') for m in ACTIVE_METADATA_LIST if m.get('pod_ip')})
        unique_images = list({m.get('container_image') for m in ACTIVE_METADATA_LIST if m.get('container_image')})
        unique_services = list({str(m.get('service_id')) for m in ACTIVE_METADATA_LIST if m.get('service_id')})
        unique_hashes = list({m.get('pod_template_hash') for m in ACTIVE_METADATA_LIST if m.get('pod_template_hash')})
        unique_hosts = list({m.get('hostname') for m in ACTIVE_METADATA_LIST if m.get('hostname')})

        for sig in signals_list:
            sig['pod_names'] = unique_pods
            sig['pod_ips'] = unique_ips
            sig['container_images'] = unique_images
            sig['service_ids'] = unique_services
            sig['pod_template_hashes'] = unique_hashes
            sig['hosts'] = unique_hosts

            # DB ve Centroid hatalarında latency darboğazı simülasyonu
            template_text = str(sig.get('template', '')).lower()
            if any(term in template_text for term in ('operationalerror', 'connection', 'centroid', 'psycopg2', 'closed')):
                sig['avg_latency'] = 3450.0  # ms
            elif 'nosuchkey' in template_text:
                sig['avg_latency'] = 820.0   # ms
            else:
                sig['avg_latency'] = 45.0    # ms

        for inc in incidents_list:
            ctx = inc.setdefault('context', {})
            ctx['pod_names'] = unique_pods
            ctx['service_ids'] = unique_services
            ctx['hosts'] = unique_hosts

    return _orig_expert_analyze(self, incidents_list, correlations, signals_list, *args, **kwargs)

ExpertRCAEngine.analyze = patched_expert_analyze


@dataclass
class DummyEvidence:
    ordinal: int = 0
    included: bool = True


def print_pipeline_layers(sonuc: dict):
    parser_items = sonuc.get('pipeline_trace', {}).get('parser', {}).get('items', [])
    template_items = sonuc.get('pipeline_trace', {}).get('template', {}).get('items', [])
    qualified_signals = sonuc.get('qualified_signals', [])
    correlations = sonuc.get('correlations', [])
    incidents = sonuc.get('incidents', [])
    rca_list = sonuc.get('rca', [])

    print("\n" + "=" * 70)
    print(f"1. KATMAN: TEMPLATED CANONICAL EVENTS ({len(parser_items)} Olay)")
    print("=" * 70)
    for idx, event in enumerate(parser_items, start=1):
        tpl_info = template_items[idx - 1] if idx - 1 < len(template_items) else {}
        attrs = event.get('attributes') or {}
        raw_preview = (event.get('raw') or '').strip().replace('\n', ' ')
        if len(raw_preview) > 85:
            raw_preview = raw_preview[:85] + "..."

        print(f"\n[EVENT #{idx}] ID: {event.get('event_id')}")
        print(f"  ├─ Ham Log (Özet)    : {raw_preview}")
        print(f"  ├─ Log Seviyesi      : {event.get('severity')}")
        print(f"  ├─ Zengin Attributes : Pod={attrs.get('pod_name')}, ServiceID={attrs.get('service_id')}, Latency={attrs.get('avg_latency')}ms")
        print(f"  └─ Template Kalıbı   : {tpl_info.get('template')}")

    print("\n" + "=" * 70)
    print(f"2. KATMAN: QUALIFIED SIGNALS ({len(qualified_signals)} Nitelikli Sinyal)")
    print("=" * 70)
    for idx, sig in enumerate(qualified_signals, start=1):
        events_str = ", ".join(sig.get('event_ids', []))
        evidence_str = ", ".join(sig.get('qualification_evidence', []))
        print(f"\n[SIGNAL #{idx}] ID: {sig.get('signal_id')}")
        print(f"  ├─ Bileşen / Scope   : {sig.get('component')}")
        print(f"  ├─ Zengin Metrikler  : Pods={sig.get('pod_names')}, ServiceIDs={sig.get('service_ids')}, Avg Latency={sig.get('avg_latency')}ms")
        print(f"  ├─ Nitelik Skoru     : {sig.get('qualification_score')} ({sig.get('qualification_reason')})")
        print(f"  ├─ Nitelik Kanıtları : {evidence_str}")
        print(f"  └─ Template Kalıbı   : {str(sig.get('template', ''))[:85]}...")

    print("\n" + "=" * 70)
    print(f"3. KATMAN: CORRELATIONS ({len(correlations)} Korelasyon Bağı)")
    print("=" * 70)
    for idx, corr in enumerate(correlations, start=1):
        src = corr.get('source', 'N/A')
        target = corr.get('target', 'N/A')
        score = corr.get('score', 1.0)
        evidence = ", ".join(corr.get('evidence', []))
        print(f"  [BAĞ #{idx}] Skor: {score} ({evidence})\n    ├─ Kaynak: {src}\n    └─ Hedef : {target}")

    print("\n" + "=" * 70)
    print(f"4. KATMAN: INCIDENTS ({len(incidents)} Olay)")
    print("=" * 70)
    for idx, inc in enumerate(incidents, start=1):
        start_dt = datetime.fromtimestamp(inc.get('start_ms', 0) / 1000, tz=timezone.utc).isoformat() if inc.get('start_ms') else 'N/A'
        end_dt = datetime.fromtimestamp(inc.get('end_ms', 0) / 1000, tz=timezone.utc).isoformat() if inc.get('end_ms') else 'N/A'
        root_info = inc.get('probable_root', {})
        print(f"\n[INCIDENT #{idx}] ID: {inc.get('incident_id')}")
        print(f"  ├─ Durum / Korelasyon: {inc.get('status')} | Güven: {inc.get('confidence')}")
        print(f"  ├─ Etkilenen Servisler: {', '.join(inc.get('components', []))}")
        print(f"  └─ Muhtemel Kök Sinyal: {root_info.get('signal_id')}")

    print("\n" + "=" * 70)
    print(f"5. KATMAN: ROOT CAUSE ANALYSIS (RCA) ({len(rca_list)} RCA Raporu)")
    print("=" * 70)
    for idx, rca in enumerate(rca_list, start=1):
        candidates = rca.get('root_cause_candidates', [])
        top_cause = candidates[0] if candidates else {}
        print(f"\n[RCA #{idx}] Hedef Incident: {rca.get('incident_id')}")
        print(f"  ├─ Analiz Modu / Durum : {rca.get('analysis_source')} ({rca.get('rca_status')})")
        print(f"  ├─ 1. Kök Neden Sinyali: {top_cause.get('signal_id')}")
        print(f"  ├─ Kök Neden Skoru     : {top_cause.get('score')} / 1.0")
        print(f"  └─ Dayanak Kanıtlar    : {', '.join(top_cause.get('evidence', []))}")

    case_analysis = sonuc.get("case_analysis")
    if case_analysis:
        print("\n" + "-" * 70)
        print("AI RCA UZMAN RAPORU (case_analysis):")
        print("-" * 70)
        print(json.dumps(case_analysis, indent=2, ensure_ascii=False, default=str))

    print("\n" + "=" * 70)


def fetch_and_test_opensearch(
    namespace: str, 
    workload: str, 
    log_limit: int = 5, 
    minutes_back: int = 60, 
    source_timezone: str = "UTC",
    use_ai_rca: bool = False
):
    global ACTIVE_METADATA_LIST
    load_environment()

    # Önbelleği temizle (Taze AI analizi yapması için)
    for cache_file in [Path("data/policy/rca_analysis_cache.json"), Path("src/backend/data/policy/rca_analysis_cache.json")]:
        if cache_file.exists():
            try:
                cache_file.unlink()
            except Exception:
                pass

    print(f"\n[DEBUG] OpenSearch bağlantısı hazırlanıyor...")
    print(f"[DEBUG] Hedef Namespace: '{namespace}' | Workload: '{workload}'")
    
    try:
        config = load_opensearch_connection(log_limit)
    except Exception as e:
        print(f"[HATA] OpenSearch bağlantı ayarları okunamadı: {e}")
        return

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=minutes_back)

    try:
        with OpenSearchClient(config) as client:
            source = OpenSearchSource(client)
            page = source.read_page(
                start=start_time,
                end=end_time,
                namespace=namespace,
                workload=workload,
                page_size=log_limit,
                cursor=None,
            )
            log_records = page.records
    except Exception as e:
        print(f"[HATA] OpenSearch sorgusunda hata oluştu: {e}")
        return

    print(f"[SUCCESS] OpenSearch'ten {len(log_records)} kayıt başarıyla çekildi.")
    if not log_records:
        return

    # Metadata'ları yapılandır
    ACTIVE_METADATA_LIST = []
    for rec in log_records:
        meta_dict = dict(rec.metadata)
        pod_ip = meta_dict.get('pod_ip', '10.0.0.1')
        pod_hash = meta_dict.get('pod_template_hash', 'default')
        pod_name = f"{workload}-{pod_hash}-{pod_ip.replace('.', '')[-5:]}"
        meta_dict['pod_name'] = pod_name
        ACTIVE_METADATA_LIST.append(meta_dict)

    print("\n" + "-" * 50)
    print("OPENSEARCH METADATA KONTROLÜ (İlk 2 Log)")
    print("-" * 50)
    for i, meta in enumerate(ACTIVE_METADATA_LIST[:2]):
        print(f"Log #{i+1} Pod Name : {meta.get('pod_name')}")
        print(f"ServiceID / IP     : {meta.get('service_id')} / {meta.get('pod_ip')}\n")

    print(f"[PIPELINE] FullAIOpsPipelineV2 çalıştırılıyor (AI RCA: {use_ai_rca})...")
    pipeline = FullAIOpsPipelineV2(use_ai_rca=use_ai_rca)
    
    logical_events = []
    for record in log_records:
        event = AssembledEvent(
            stream_key=record.stream_identity,
            records=(record,),
            evidence=(DummyEvidence(),),  
            policy=None,
            text=record.raw_text,
            emission_reason='debug_bypass'
        )
        logical_events.append(event)

    timestamp_policy = TimestampSourcePolicy(source_timezone)
    sonuc = pipeline._process_logical_events(logical_events, timestamp_policy=timestamp_policy)

    # 1. Katmandaki canonical event'lere de görsel kontrol için metadata ekleyelim
    parser_items = sonuc.get('pipeline_trace', {}).get('parser', {}).get('items', [])
    for idx, item in enumerate(parser_items):
        if idx < len(ACTIVE_METADATA_LIST):
            attrs = item.setdefault('attributes', {})
            attrs.update(ACTIVE_METADATA_LIST[idx])
            attrs['avg_latency'] = 3450.0

    print_pipeline_layers(sonuc)

    print("\n" + "=" * 55)
    print("PIPELINE ISLEME SONUCU:")
    print("=" * 55)
    stats = sonuc.get('stats', {})
    print(f"Nitelikli Sinyal Sayısı  : {len(sonuc.get('qualified_signals', []))}")
    print(f"Oluşan Olay (Incident)   : {len(sonuc.get('incidents', []))}")
    print("=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OpenSearch Parametrik Debug Runner")
    parser.add_argument("--namespace", type=str, default="ai-document-assistant", help="Kubernetes Namespace")
    parser.add_argument("--workload", type=str, default="aida-document-digest-consumer-dds", help="Workload adı")
    parser.add_argument("--limit", type=int, default=5, help="Çekilecek maksimum log adedi")
    parser.add_argument("--minutes", type=int, default=60, help="Geriye dönük kaç dakika taranacak")
    parser.add_argument("--timezone", type=str, default="UTC", help="Log zaman damgaları için saat dilimi")
    parser.add_argument("--ai-rca", action="store_true", help="Yapay zeka destekli RCA ajanını aktif eder")
    args = parser.parse_args()

    fetch_and_test_opensearch(
        namespace=args.namespace,
        workload=args.workload,
        log_limit=args.limit,
        minutes_back=args.minutes,
        source_timezone=args.timezone,
        use_ai_rca=args.ai_rca
    )