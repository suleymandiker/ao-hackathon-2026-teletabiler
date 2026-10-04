# -*- coding: utf-8 -*-
"""
Segmentation Layer - Sampling Teaching / Debug Runner

Amaç:
- Production kodunu değiştirmeden StratifiedSampler'ın
  log dosyasından nasıl örnek aldığını görmek.
- Discovery sampling ile signature sampling arasındaki
  farkı adım adım incelemek.
"""

import os
import sys
from pathlib import Path


# -------------------------------------------------------------------
# IMPORT PATH
# -------------------------------------------------------------------

THIS_DIR = Path(__file__).resolve().parent

if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))


from segmentation_layer.segmentation_pipeline import (
    SegmentationPipeline,
    StratifiedSampler,
)


# -------------------------------------------------------------------
# EKRAN AYRAÇLARI
# -------------------------------------------------------------------

BAR = "=" * 100
SUB = "-" * 100


def getData():

    file_path = (
        r"D:\Dev\hackathon\ao-hackathon-2026-teletabiler"
        r"\src\backend\bigData.log"
    )

    # ================================================================
    # 1. SAMPLER OLUŞTUR
    # ================================================================

    sampler = StratifiedSampler()

    print("\n" + BAR)
    print("STEP 1 - STRATIFIED SAMPLER")
    print(BAR)

    print(f"Dosya          : {file_path}")
    print(f"Dosya var mı?  : {os.path.exists(file_path)}")

    if not os.path.exists(file_path):
        print("\nHATA: Dosya bulunamadı.")
        return

    print(f"Dosya boyutu   : {os.path.getsize(file_path):,} bytes")

    print()
    print("Sampler ayarları:")
    print(f"  num_chunks    : {sampler.num_chunks}")
    print(f"  chunk_size    : {sampler.chunk_size}")
    print(f"  sample_budget : {sampler.sample_budget}")

    # ================================================================
    # 2. DISCOVERY SAMPLE AL
    # ================================================================

    print("\n" + BAR)
    print("STEP 2 - sampler.sample(file_path)")
    print(BAR)

    samples, file_size = sampler.sample(file_path)

    print(f"File size       : {file_size:,} bytes")
    print(f"Sample sayısı   : {len(samples)}")

    # ================================================================
    # 3. SAMPLE'LARI CHUNK OLARAK GÖSTER
    # ================================================================

    print("\n" + BAR)
    print("STEP 3 - ALINAN DISCOVERY SAMPLE'LARI")
    print(BAR)

    chunk_size = sampler.chunk_size

    for sample_index, sample in enumerate(samples):

        # Her chunk'ın başında başlık yaz
        if sample_index % chunk_size == 0:

            chunk_no = (sample_index // chunk_size) + 1

            print()
            print(SUB)

            print(
                f"CHUNK {chunk_no}/{sampler.num_chunks}"
                f"   | sample index "
                f"{sample_index + 1}-"
                f"{min(sample_index + chunk_size, len(samples))}"
            )

            print(SUB)

        # Chunk içerisindeki sıra
        line_in_chunk = (sample_index % chunk_size) + 1

        # Çok uzun log satırları terminali mahvetmesin
        preview = sample

        if len(preview) > 300:
            preview = preview[:300] + " ... [TRUNCATED]"

        print(
            f"[{sample_index + 1:03}] "
            f"[chunk-line {line_in_chunk:02}] "
            f"{preview}"
        )

    # ================================================================
    # 4. DISCOVERY SAMPLING ÖZETİ
    # ================================================================

    print("\n" + BAR)
    print("STEP 4 - DISCOVERY SAMPLING SUMMARY")
    print(BAR)

    print(f"Dosya boyutu        : {file_size:,} bytes")
    print(f"Toplam sample       : {len(samples)}")
    print(f"Chunk sayısı        : {sampler.num_chunks}")
    print(f"Chunk başına hedef  : {sampler.chunk_size}")
    print(f"Sample budget       : {sampler.sample_budget}")

    print()
    print("Sampling tamamlandı.")

    if len(samples) < sampler.sample_budget:
        print(
            "Not: Sample sayısı budget'tan küçük. "
            "Dosya küçükse tamamı okunmuş olabilir."
        )

    # ================================================================
    # 5. PIPELINE OLUŞTUR
    # ================================================================

    print("\n" + BAR)
    print("STEP 5 - SegmentationPipeline()")
    print(BAR)

    print(
        "Şimdi production SegmentationPipeline nesnesi oluşturuluyor.\n"
        "Signature işlemlerini bu nesne üzerinden çağıracağız."
    )

    pipeline = SegmentationPipeline()

    print()
    print("Pipeline oluşturuldu.")

    # ================================================================
    # 6. SIGNATURE SAMPLE
    # ================================================================

    print("\n" + BAR)
    print("STEP 6 - pipeline._signature_sample(file_path)")
    print(BAR)

    print(
        "Amaç: AI discovery için sample almak DEĞİL.\n"
        "Bu sample, log formatının registry identity'sini "
        "oluşturmak için kullanılır."
    )

    signature_samples = pipeline._signature_sample(file_path)

    print()
    print(f"Signature sample sayısı : {len(signature_samples)}")

    print("\nSignature sample'ları:")

    for index, line in enumerate(signature_samples, start=1):

        preview = line

        if len(preview) > 300:
            preview = preview[:300] + " ... [TRUNCATED]"

        print(f"[{index:03}] {preview}")

    # ================================================================
    # 7. FORMAT SIGNATURE
    # ================================================================

    print("\n" + BAR)
    print("STEP 7 - pipeline._format_signature(signature_samples)")
    print(BAR)

    print(
        "Şimdi yukarıdaki stabil prefix sample kullanılarak\n"
        "log formatını temsil eden signature oluşturulacak."
    )

    signature = pipeline._format_signature(signature_samples)

    print()
    print(f"Format signature : {signature}")

    # ================================================================
    # 8. DISCOVERY SAMPLE vs SIGNATURE SAMPLE
    # ================================================================

    print("\n" + BAR)
    print("STEP 8 - DISCOVERY SAMPLE vs SIGNATURE SAMPLE")
    print(BAR)

    print(
        """
DISCOVERY SAMPLE
----------------
Fonksiyon:
    StratifiedSampler.sample(file_path)

Amaç:
    AI'ın log formatını keşfetmesi.

Büyük dosyada:
    Dosyanın farklı byte bölgelerinden
    contiguous window'lar alınır.

Kullanıldığı yer:
    HeaderDiscovery.discover(samples)


SIGNATURE SAMPLE
----------------
Fonksiyon:
    pipeline._signature_sample(file_path)

Amaç:
    Log formatının stabil registry
    identity'sini oluşturmak.

Büyük dosyada:
    Dosyanın başından bounded prefix okunur.

Kullanıldığı yer:
    pipeline._format_signature(signature_samples)
"""
    )

    print(SUB)

    print(f"Discovery sample count : {len(samples)}")
    print(f"Signature sample count : {len(signature_samples)}")
    print(f"Format signature       : {signature}")

    print(SUB)

    print(
        "\nAKIŞ:\n\n"
        "LOG FILE\n"
        "   |\n"
        "   +--> StratifiedSampler.sample()\n"
        "   |       |\n"
        "   |       +--> Discovery Samples\n"
        "   |               |\n"
        "   |               +--> HeaderDiscovery / AI\n"
        "   |\n"
        "   +--> _signature_sample()\n"
        "           |\n"
        "           +--> Signature Samples\n"
        "                   |\n"
        "                   +--> _format_signature()\n"
        "                           |\n"
        "                           +--> Registry Signature\n"
    )

    print(BAR)


if __name__ == "__main__":
    getData()