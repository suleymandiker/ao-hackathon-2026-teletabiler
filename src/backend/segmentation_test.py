# -*- coding: utf-8 -*-
"""
Segmentation Layer Teaching / Debug Runner
"""

from __future__ import annotations

import sys
from pathlib import Path


THIS_DIR = Path(__file__).resolve().parent

if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))


from segmentation_layer.segmentation_pipeline import (
    SegmentationPipeline,
    StratifiedSampler,
)


BAR = "=" * 88
SUB = "-" * 88


def getData(limit):

    file_path = (
        r"D:\Dev\hackathon\ao-hackathon-2026-teletabiler"
        r"\src\backend\logfile.log"
    )

    # ------------------------------------------------------------
    # 1. NESNELERI OLUSTUR
    # ------------------------------------------------------------

    sampler = StratifiedSampler()
    veri = SegmentationPipeline()

    # ------------------------------------------------------------
    # 2. SEGMENTATION PREPARE
    # ------------------------------------------------------------

    result = veri.prepare(file_path)

    print("\n" + BAR)
    print("PREPARE RESULT")
    print(BAR)
    print(result)

    # ------------------------------------------------------------
    # 3. SAMPLE
    # ------------------------------------------------------------

    data = sampler.sample(file_path)

    # ------------------------------------------------------------
    # 4. PREPARE SONUCUNDAN REGEX'I AL
    # ------------------------------------------------------------

    regex = result["regex"]

    print("\n" + BAR)
    print("REGEX")
    print(BAR)
    print(regex)

    # ------------------------------------------------------------
    # 5. MULTILINE ASSEMBLER
    # ------------------------------------------------------------

    print("\n" + BAR)
    print("MULTILINE ASSEMBLER")
    print(BAR)

    event_count = 0

    for record in veri.assembler.iter_event_records(
        file_path,
        regex,
    ):

        event_count += 1

        print("\n" + SUB)
        print(f"EVENT #{event_count}")
        print(SUB)

        print(f"Start line : {record['start_line']}")
        print(f"End line   : {record['end_line']}")
        print(f"Line count : {record['line_count']}")
        print(f"Header     : {record['first_line_is_header']}")
        print(f"Source     : {record['source']}")

        print("\nEVENT:")
        print(record["event"])

        print("\nDECISIONS:")

        for decision in record["decisions"]:
            print(decision)

        # LIMIT KONTROLU
        if event_count >= limit:
            break

    # ------------------------------------------------------------
    # 6. SONUC
    # ------------------------------------------------------------

    print("\n" + BAR)
    print("RESULT")
    print(BAR)

    print(f"Displayed logical event count : {event_count}")


if __name__ == "__main__":
    getData(limit=20)