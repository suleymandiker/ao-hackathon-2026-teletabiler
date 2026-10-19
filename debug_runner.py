# -*- coding: utf-8 -*-
import sys
import os
import json

base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(base_dir, 'src', 'backend'))
sys.path.insert(0, os.path.join(base_dir, 'src'))

from parser_layer.parser_pipeline import ParserPipeline
from parser_layer.format_detector.format_detector import FormatDetector

def main():
    print("=== PARSER POLICY AI DISCOVERY TESTİ BAŞLATILIYOR ===\n")
    
    # 1. FormatDetector'ın asla JSON, KV veya Syslog diyemeyeceği özel bir format
    #    (Örn: [PREFIX] ## TIMESTAMP ## LEVEL ## COMPONENT ## MESSAGE)
    ornek_loglar = [
        "CUSTOM_APP ## 2026-10-17T20:10:01.120Z ## ERROR ## BillingEngine ## Failed to process invoice #9012: Gateway Timeout",
        "CUSTOM_APP ## 2026-10-17T20:10:01.340Z ## WARN ## BillingEngine ## Retrying invoice #9012 with fallback provider",
        "CUSTOM_APP ## 2026-10-17T20:10:01.890Z ## ERROR ## BillingEngine ## Fallback provider also timed out (status 504)",
        "CUSTOM_APP ## 2026-10-17T20:10:02.100Z ## CRITICAL ## BillingEngine ## Invoice #9012 marked as FAILED - DB state inconsistent",
        "CUSTOM_APP ## 2026-10-17T20:10:02.450Z ## INFO ## BillingEngine ## Alert notification dispatched to ops on-call",
        "CUSTOM_APP ## 2026-10-17T20:10:02.890Z ## ERROR ## PaymentService ## Failed to acquire lock for account ACC-4421",
        "CUSTOM_APP ## 2026-10-17T20:10:03.110Z ## CRITICAL ## PaymentService ## Deadlock detected on resource balance_table",
    ]

    detector = FormatDetector()
    ilk_format = detector.detect(ornek_loglar[0])
    print(f"[KONTROL] Örnek log FormatDetector tarafından algılandı: '{ilk_format}'")
    
    if ilk_format != "plain_text":
        print("[HATA] Log formatı plain_text olarak algılanmadı! AI Discovery tetiklenemez.")
        return

    # 2. ParserPipeline'ı başlatıyoruz
    #    (registry_path varsayılan olarak data/policy/policy_registry.sqlite3 alır)
    pipeline = ParserPipeline(ai_enabled=True)

    print("\n[ADIM 1] ParserPipeline.prepare() çağrılıyor (AI Discovery devrede)...")
    prepare_result = pipeline.prepare(ornek_loglar)
    
    print("\n[SONUÇ] prepare() çıktısı:")
    print(json.dumps(prepare_result, indent=2, ensure_ascii=False))

    # 3. Bir log satırını yeni keşfedilen custom_parser ile ayrıştırıyoruz
    print("\n[ADIM 2] Örnek log yeni custom kural ile ayrıştırılıyor...")
    outcome = pipeline.process_with_outcome(ornek_loglar[0])
    
    print(f"  ├─ Kullanılan Parser : {outcome.parser_id}")
    print(f"  ├─ Teslimat Durumu   : {outcome.delivery}")
    print(f"  ├─ Çıkarılan Alanlar :")
    print(json.dumps(outcome.event, indent=4, ensure_ascii=False, default=str))

    print("\n[BİLGİ] SQLite tablosunu (parser_policies) şimdi kontrol edebilirsiniz.")

if __name__ == "__main__":
    main()