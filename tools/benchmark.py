# -*- coding: utf-8 -*-
"""AOP ölçüm koşusu — AI_JURI.md içindeki her sayıyı yeniden üretir.

Kullanım:
    python tools/benchmark.py              # tüm senaryolar
    python tools/benchmark.py --offline    # hiç AI çağrısı yapma

Ölçülen senaryolar:
    S1  Alarm paketi  · deterministik (Qwen kapalı)
    S2  Alarm paketi  · Qwen uzman yorumu açık
    S3  Ham log       · soğuk registry (DeepSeek format keşfi)
    S4  Ham log       · sıcak registry (AI = 0)

Sentetik alarm paketi rastgelelik içermez; aynı komut her zaman aynı
sayıları üretir.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import shutil
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "backend"))

RAW_LOG = ROOT / "tests" / "fixtures" / "sre_segmentation_test_same_format_changed_content.log"

# Sentetik senaryo: billing-db diski dolar -> billing-api yazamaz ->
# payment-api zaman aşımına uğrar -> order-gw 502 döner.
# Bağımsız arka plan gürültüsü ayrı bir veri merkezinde üretilir.
SCENARIO = [
    # (offset_sn, host, servis, severity, alarm_type, mesaj, adet)
    (0, "db-01", "billing-db", 5, "disk_full", "data volume 98% full", 6),
    (40, "app-01", "billing-api", 4, "db_write_fail", "write to billing-db failed", 8),
    (70, "app-02", "payment-api", 4, "timeout", "upstream billing-api timeout", 12),
    (90, "web-01", "order-gw", 4, "http_5xx", "502 from payment-api", 10),
    (0, "mon-01", "metrics-agent", 1, "cpu_high", "cpu 71%", 30),
    (0, "bk-01", "backup-svc", 2, "backup_warn", "nightly backup slow", 20),
    (0, "ntp-01", "ntp-svc", 2, "ntp_drift", "drift 12ms", 5),
]

INVENTORY = [
    ("db-01", "billing-db", "DC1", "R12", "prod", "yuksek"),
    ("app-01", "billing-api", "DC1", "R12", "prod", "yuksek"),
    ("app-02", "payment-api", "DC1", "R13", "prod", "yuksek"),
    ("web-01", "order-gw", "DC1", "R14", "prod", "orta"),
    ("mon-01", "metrics-agent", "DC2", "R01", "prod", "dusuk"),
    ("bk-01", "backup-svc", "DC2", "R02", "prod", "dusuk"),
    ("ntp-01", "ntp-svc", "DC2", "R03", "prod", "dusuk"),
]

DEPENDENCIES = [
    ("billing-api", "billing-db", "db", "yuksek"),
    ("payment-api", "billing-api", "http", "yuksek"),
    ("order-gw", "payment-api", "http", "yuksek"),
]

BEKLENEN_KOK_SERVIS = "billing-db"
BEKLENEN_KOK_ALARM = "disk_full"


def _csv(rows, header):
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue()


def build_package(target: Path) -> int:
    base = datetime(2026, 9, 16, 10, 0, 0)
    host_meta = {h: (dc, rack, env) for h, _s, dc, rack, env, _k in INVENTORY}
    alarms = []
    for offset, host, service, severity, alarm_type, message, count in SCENARIO:
        dc, rack, env = host_meta[host]
        step = 5 if severity >= 4 else 7
        for i in range(count):
            alarms.append({
                "alarm_id": f"ALM-{len(alarms) + 1:05d}",
                "timestamp": (base + timedelta(seconds=offset + i * step)).isoformat() + "Z",
                "host": host,
                "service": service,
                "severity": severity,
                "alarm_type": alarm_type,
                "message": message,
                "source_system": "nagios",
                "tags": {"veri_merkezi": dc, "kabin": rack, "ortam": env},
            })

    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("alarms.json", json.dumps(alarms, ensure_ascii=False, indent=1))
        z.writestr("host_inventory.csv", _csv(
            INVENTORY, ["host", "servis", "veri_merkezi", "kabin", "ortam", "is_kritikligi"]))
        z.writestr("service_dependencies.csv", _csv(
            DEPENDENCIES, ["kaynak_servis", "hedef_servis", "bagimlilik_tipi", "kritiklik"]))
    return len(alarms)


def run(label: str, fn):
    start = time.time()
    result = fn()
    return {"label": label, "seconds": round(time.time() - start, 3), "result": result}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offline", action="store_true",
                        help="Hiç AI çağrısı yapma (S2 ve S3 deterministik moda düşer).")
    args = parser.parse_args()

    from full_pipeline_v2 import FullAIOpsPipelineV2

    workdir = Path(tempfile.mkdtemp(prefix="aop_bench_"))
    pkg = workdir / "alarm_package.zip"
    registry = workdir / "policy_registry.sqlite3"
    os.environ["AIOPS_POLICY_REGISTRY_PATH"] = str(registry)

    alarm_count = build_package(pkg)
    print(f"\nSentetik alarm paketi: {pkg}  ({alarm_count} alarm)\n")

    runs = []
    runs.append(run("S1 · Alarm paketi · deterministik", lambda: FullAIOpsPipelineV2(
        use_ai_rca=False,
        template_state=str(workdir / "tpl.json"),
        drain_state=str(workdir / "tpl.bin")).process_package(str(pkg))))

    runs.append(run("S2 · Alarm paketi · Qwen açık", lambda: FullAIOpsPipelineV2(
        use_ai_rca=not args.offline,
        template_state=str(workdir / "tpl.json"),
        drain_state=str(workdir / "tpl.bin")).process_package(str(pkg))))

    if RAW_LOG.exists():
        raw_lines = sum(1 for _ in RAW_LOG.open(encoding="utf-8", errors="ignore"))
        runs.append(run("S3 · Ham log · soğuk registry", lambda: FullAIOpsPipelineV2(
            use_ai_rca=False,
            template_state=str(workdir / "raw.json"),
            drain_state=str(workdir / "raw.bin")).process_file(str(RAW_LOG))))
        runs.append(run("S4 · Ham log · sıcak registry", lambda: FullAIOpsPipelineV2(
            use_ai_rca=False,
            template_state=str(workdir / "raw.json"),
            drain_state=str(workdir / "raw.bin")).process_file(str(RAW_LOG))))
    else:
        raw_lines = 0
        print(f"UYARI: {RAW_LOG} bulunamadı; S3/S4 atlandı.")

    print("\n" + "=" * 96)
    print("SONUÇ TABLOSU")
    print("=" * 96)
    head = f"{'Senaryo':<36}{'sn':>7}{'olay':>7}{'sinyal':>8}{'nitelik':>9}{'gürültü':>9}{'korel.':>8}{'incident':>10}"
    print(head)
    print("-" * 96)
    for item in runs:
        s = item["result"]["stats"]
        print(f"{item['label']:<36}{item['seconds']:>7.2f}"
              f"{s.get('templated', 0):>7}{s.get('signal_candidates', 0):>8}"
              f"{s.get('qualified_signals', 0):>9}{s.get('noise_suppressed', 0):>9}"
              f"{s.get('correlations', 0):>8}{s.get('incidents', 0):>10}")
    print("=" * 96)

    pkg_stats = runs[0]["result"]["stats"]
    incidents = runs[0]["result"]["incidents"]
    signals = pkg_stats.get("signal_candidates", 0)
    noise = pkg_stats.get("noise_suppressed", 0)

    print("\nTÜRETİLMİŞ METRİKLER (S1)")
    print(f"  Gürültü azaltma          : {100 * noise / max(1, signals):.1f}%  "
          f"({noise}/{signals} aday bastırıldı)")
    print(f"  Alarm → olay yoğunlaşması: {alarm_count} alarm → {len(incidents)} olay  "
          f"(%{100 * (1 - len(incidents) / max(1, alarm_count)):.1f} azalma)")
    if raw_lines:
        seg = runs[2]["result"]["stats"].get("segmented", 0) if len(runs) > 2 else 0
        print(f"  Ham log segmentasyonu    : {raw_lines} fiziksel satır → {seg} mantıksal olay")
        print(f"  Registry hızlanması      : {runs[2]['seconds']:.2f}s → {runs[3]['seconds']:.3f}s  "
              f"({runs[2]['seconds'] / max(0.001, runs[3]['seconds']):.0f}x)")

    print("\nDOĞRULUK KONTROLLERİ")
    ok = True

    root = (incidents[0].get("probable_root") or {}) if incidents else {}
    checks = [
        ("K2 kök varlık doğru", root.get("entity") == BEKLENEN_KOK_SERVIS,
         f"{root.get('entity')} (beklenen {BEKLENEN_KOK_SERVIS})"),
        ("K2 kök alarm tipi doğru", root.get("alarm_type") == BEKLENEN_KOK_ALARM,
         f"{root.get('alarm_type')} (beklenen {BEKLENEN_KOK_ALARM})"),
        ("K3 tek nedensel zincir tek olay", len(incidents) == 1,
         f"{len(incidents)} olay"),
        ("K1 arka plan gürültüsü bastırıldı",
         all(not s.get("qualified") for s in runs[0]["result"]["signals"]
             if s.get("alarm_type") in {"cpu_high", "backup_warn", "ntp_drift"}),
         "cpu_high / backup_warn / ntp_drift"),
        ("K4 S1 ve S2 deterministik kısmı birebir aynı",
         runs[0]["result"]["stats"].get("incidents") == runs[1]["result"]["stats"].get("incidents")
         and runs[0]["result"]["stats"].get("correlations") == runs[1]["result"]["stats"].get("correlations"),
         "olay ve korelasyon sayıları"),
        ("K5 AI olmadan da olay üretildi", len(incidents) >= 1, "S1 Qwen kapalı"),
        ("K6 her nitelikli sinyalde kanıt var",
         all(s.get("qualification_evidence") for s in runs[0]["result"]["qualified_signals"]),
         "qualification_evidence"),
    ]
    for name, passed, detail in checks:
        ok &= passed
        print(f"  [{'GEÇTİ' if passed else 'KALDI'}] {name:<42} {detail}")

    case = runs[1]["result"].get("case_analysis")
    if case:
        print(f"\nQWEN UZMAN YORUMU (S2) · model={case.get('analysis_model')} · "
              f"{case.get('ai_duration_seconds')}s · {case.get('ai_usage', {}).get('total_tokens')} token")
        print(f"  güven={case.get('guven')} · nedensellik={case.get('nedensellik_durumu')}")
        print(f"  hipotez: {case.get('kok_neden_hipotezi')}")
    elif not args.offline:
        print(f"\nQwen çıktısı alınamadı: {runs[1]['result'].get('case_analysis_error')}")
        print("  Deterministik RCA korunuyor — K5 kanıtı.")

    shutil.rmtree(workdir, ignore_errors=True)
    print()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
