# -*- coding: utf-8 -*-
"""
Segmentation Layer Teaching / Debug Runner

Amaç:
- Production segmentation kodunu değiştirmeden gerçek bileşenleri tek tek çalıştırmak.
- Bir log dosyasının sample -> signature -> registry -> discovery -> validation
  -> classification -> assembly aşamalarından nasıl geçtiğini öğretici biçimde göstermek.

Önerilen konum:
    src/backend/segmentation_debug.py

Örnek:
    python src/backend/segmentation_debug.py app.log --lines 73440:73500
    python src/backend/segmentation_debug.py app.log --summary
    python src/backend/segmentation_debug.py app.log --force-rediscovery --lines 1:50

Not:
- --force-rediscovery verilmezse mevcut verified registry policy varsa AI çağrılmaz.
- Registry miss durumunda AI açıksa gerçek HeaderDiscovery kullanılır.
- --regex ile regex elle verilirse registry/discovery atlanır.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


# Bu dosya src/backend altında ise segmentation_layer doğrudan import edilir.
THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from segmentation_layer.header_classifier import HeaderClassifier
from segmentation_layer.header_discovery import HeaderDiscovery
from segmentation_layer.multiline_assembler import MultilineAssembler
from segmentation_layer.policy_registry import SegmentationPolicyRegistry
from segmentation_layer.regex_validator import RegexValidator
from segmentation_layer.segmentation_pipeline import SegmentationPipeline


BAR = "=" * 88
SUB = "-" * 88


def title(step: str, name: str) -> None:
    print(f"\n{BAR}")
    print(f"{step} — {name}")
    print(BAR)


def kv(name: str, value: Any) -> None:
    print(f"{name:<30}: {value}")


def call_trace(py_file: str, function: str, before: Any = None, after: Any = None) -> None:
    """Öğretici çağrı izi: hangi dosya/fonksiyon çağrıldı ve girdiden ne çıktı."""
    print(f"\n[CALL] {py_file}  →  {function}")
    if before is not None:
        print(f"       BEFORE : {before}")
    if after is not None:
        print(f"       AFTER  : {after}")


def parse_range(value: Optional[str]) -> Optional[Tuple[int, int]]:
    if not value:
        return None
    m = re.fullmatch(r"\s*(\d+)\s*:\s*(\d+)\s*", value)
    if not m:
        raise argparse.ArgumentTypeError("--lines formatı START:END olmalı. Örn: 73440:73500")
    start, end = int(m.group(1)), int(m.group(2))
    if start < 1 or end < start:
        raise argparse.ArgumentTypeError("Geçersiz satır aralığı.")
    return start, end


def count_physical_lines(file_path: str) -> Tuple[int, int, int]:
    total = nonempty = blank = 0
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            total += 1
            if raw.strip():
                nonempty += 1
            else:
                blank += 1
    return total, nonempty, blank


def read_range(file_path: str, line_range: Tuple[int, int]) -> Iterable[Tuple[int, str]]:
    start, end = line_range
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for no, raw in enumerate(f, 1):
            if no < start:
                continue
            if no > end:
                break
            yield no, raw.rstrip("\r\n")


def decision_dict(d) -> Dict[str, Any]:
    return {
        "classification": d.classification,
        "start_new_event": d.start_new_event,
        "regex_match": d.regex_match,
        "reason": d.reason,
        "confidence": d.confidence,
    }


def print_samples(samples, max_show: int = 8) -> None:
    print(f"\nİlk {min(len(samples), max_show)} sample:")
    for i, line in enumerate(samples[:max_show], 1):
        preview = line if len(line) <= 180 else line[:177] + "..."
        print(f"  [{i:02d}] {preview}")


def explain_decision(line_no: int, line: str, d, after_blank: bool) -> None:
    print(f"\nLINE {line_no}")
    print(SUB)
    print(f"RAW                  : {line[:500]}")
    print(f"after_blank          : {after_blank}")
    print(f"regex_match          : {'YES' if d.regex_match else 'NO'}")
    print(f"classification       : {d.classification}")
    print(f"start_new_event      : {'YES' if d.start_new_event else 'NO'}")
    print(f"reason               : {d.reason}")
    print(f"confidence           : {d.confidence:.2f}")

    if not line.strip():
        result = "BLANK → event hemen bitmez; after_blank=True olur ve sonraki satır bağlamla değerlendirilir."
    elif d.start_new_event:
        result = "NEW EVENT → bu satır yeni logical event başlatır."
    else:
        result = "CONTINUATION → aktif event varsa ona eklenir."
    print(f"SONUÇ                : {result}")


def get_parser_fn():
    try:
        from parser_layer.parser_pipeline import ParserPipeline
        parser = ParserPipeline()
        return parser.process
    except Exception as exc:
        print(f"[INFO] ParserPipeline kullanılamadı; parser validation atlanacak: {exc}")
        return None


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Segmentation layer'ı öğretici şekilde adım adım trace eder."
    )
    ap.add_argument("log_file", help="İncelenecek log dosyası")
    ap.add_argument(
        "--lines",
        type=parse_range,
        default=None,
        help="Detaylı classification gösterilecek fiziksel satırlar. Örn: 73440:73500",
    )
    ap.add_argument(
        "--summary",
        action="store_true",
        help="Satır bazlı ayrıntıyı kapat; yalnız aşama özetlerini göster.",
    )
    ap.add_argument(
        "--force-rediscovery",
        action="store_true",
        help="Registry hit olsa bile AI discovery yap.",
    )
    ap.add_argument(
        "--no-ai",
        action="store_true",
        help="AI discovery çağırma. Registry veya --regex gerekir.",
    )
    ap.add_argument(
        "--regex",
        default=None,
        help="Discovery/registry yerine doğrudan test edilecek event header regex.",
    )
    ap.add_argument(
        "--registry-path",
        default=None,
        help="Opsiyonel policy_registry.sqlite3 yolu.",
    )
    ap.add_argument(
        "--sample-show",
        type=int,
        default=8,
        help="Ekranda gösterilecek sample satır sayısı.",
    )
    ap.add_argument(
        "--event-limit",
        type=int,
        default=10,
        help="Seçilen satır aralığıyla kesişen en fazla kaç assembled event gösterilsin.",
    )
    args = ap.parse_args()

    file_path = os.path.abspath(args.log_file)
    if not os.path.isfile(file_path):
        print(f"[ERROR] Dosya bulunamadı: {file_path}", file=sys.stderr)
        return 2

    # Gerçek production bileşenleri.
    pipeline = SegmentationPipeline(
        discovery_sample_lines=150,
        enable_ai=not args.no_ai,
        registry_path=args.registry_path,
    )
    classifier = HeaderClassifier()
    assembler = MultilineAssembler(classifier=classifier)
    validator = RegexValidator()
    registry: SegmentationPolicyRegistry = pipeline.policy_registry

    title("STEP 0", "INPUT")
    kv("Dosya", file_path)
    kv("Boyut (byte)", os.path.getsize(file_path))
    total, nonempty, blank = count_physical_lines(file_path)
    kv("Physical lines", total)
    kv("Non-empty lines", nonempty)
    kv("Blank lines", blank)

    title("STEP 1", "STRATIFIED SAMPLING")
    call_trace(
        "segmentation_pipeline.py",
        "StratifiedLineSampler.sample(file_path)",
        before=f"log_file={file_path}",
    )
    samples, file_size = pipeline.sampler.sample(file_path)
    call_trace(
        "segmentation_pipeline.py",
        "StratifiedLineSampler.sample(file_path)",
        after=f"physical log → {len(samples)} representative sample lines, file_size={file_size}",
    )
    kv("Discovery sample count", len(samples))
    kv("Sample budget", pipeline.discovery_sample_lines)
    kv("File size", file_size)
    print("Amaç: Full dosyayı AI'a vermeden dosyanın farklı bölgelerinden temsilî örnek toplamak.")
    print_samples(samples, max_show=max(0, args.sample_show))

    title("STEP 2", "FORMAT SIGNATURE")
    call_trace(
        "segmentation_pipeline.py",
        "_signature_sample(file_path)",
        before=f"log_file={file_path}",
    )
    signature_samples = pipeline._signature_sample(file_path)
    call_trace(
        "segmentation_pipeline.py",
        "_signature_sample(file_path)",
        after=f"{len(signature_samples)} signature sample lines",
    )

    call_trace(
        "segmentation_pipeline.py",
        "_format_signature(samples)",
        before=f"{len(signature_samples)} sampled physical lines",
    )
    signature = pipeline._format_signature(signature_samples)
    call_trace(
        "segmentation_pipeline.py",
        "_format_signature(samples)",
        after=f"signature={signature}",
    )
    kv("Signature sample count", len(signature_samples))
    kv("Format signature", signature)
    print("Amaç: Aynı log formatını tekrar gördüğümüzde verified policy'yi yeniden kullanabilmek.")

    candidate_regex: Optional[str] = args.regex
    candidate_source = "manual" if candidate_regex else None
    discovery_result: Optional[Dict[str, Any]] = None

    title("STEP 3", "POLICY REGISTRY LOOKUP")
    if candidate_regex:
        print("--regex verildiği için registry lookup karar için kullanılmadı.")
    elif args.force_rediscovery:
        print("--force-rediscovery aktif → registry policy bilerek kullanılmayacak.")
        call_trace("policy_registry.py", "SegmentationPolicyRegistry.get(signature)",
                   before=f"signature={signature}")
        policy, level = registry.get(signature)
        call_trace("policy_registry.py", "SegmentationPolicyRegistry.get(signature)",
                   after=f"level={level}, policy_found={bool(policy)}")
        kv("Registry'de policy var mı?", bool(policy))
        kv("Registry level", level)
    else:
        call_trace("policy_registry.py", "SegmentationPolicyRegistry.get(signature)",
                   before=f"signature={signature}")
        policy, level = registry.get(signature)
        call_trace("policy_registry.py", "SegmentationPolicyRegistry.get(signature)",
                   after=f"level={level}, policy_found={bool(policy)}")
        kv("Registry level", level)
        kv("Policy found", bool(policy))
        if policy:
            candidate_regex = policy["regex"]
            candidate_source = f"registry:{level}"
            kv("Verified", policy.get("verified"))
            kv("Stored source", policy.get("source"))
            kv("Stored coverage", policy.get("coverage_score"))
            kv("Stored parser success", policy.get("parser_success"))
            kv("Stored event count", policy.get("event_count"))
            kv("Regex", candidate_regex)
            print("SONUÇ: Format daha önce doğrulanmış → AI discovery gerekmiyor.")
        else:
            print("SONUÇ: Registry MISS → yeni format discovery aşamasına geçilecek.")

    if not candidate_regex:
        title("STEP 4", "HEADER DISCOVERY")
        if args.no_ai:
            print("[STOP] Registry policy yok ve --no-ai aktif.")
            print("Devam etmek için --regex '^...' ver veya AI'ı etkin bırak.")
            return 3

        discovery: HeaderDiscovery = pipeline.discovery
        print(f"AI'a gönderilen temsilî sample sayısı: {len(samples)}")
        print("Amaç: Sadece NEW top-level event başlangıcını yakalayan candidate regex keşfetmek.")
        call_trace(
            "header_discovery.py",
            "HeaderDiscovery.discover(samples)",
            before=f"{len(samples)} representative sample lines; header regex henüz yok",
        )
        discovery_result = discovery.discover(samples)
        call_trace(
            "header_discovery.py",
            "HeaderDiscovery.discover(samples)",
            after=f"candidate_regex={discovery_result.get('event_header_regex')!r}, confidence={discovery_result.get('confidence')}",
        )
        candidate_regex = discovery_result["event_header_regex"]
        candidate_source = "ai-discovery"
        kv("Candidate regex", candidate_regex)
        kv("Confidence", discovery_result.get("confidence"))
        kv("Multiline detected", discovery_result.get("is_multiline_detected"))
        kv("Explanation", discovery_result.get("explanation"))
    else:
        title("STEP 4", "HEADER DISCOVERY")
        print(f"Atlandı → regex kaynağı: {candidate_source}")

    assert candidate_regex is not None

    title("STEP 5", "FULL-FILE REGEX VALIDATION")
    print("Amaç: AI/registry/manual regex'i full dosyada deterministic olarak sınamak.")
    parser_fn = get_parser_fn()
    call_trace(
        "regex_validator.py",
        "RegexValidator.validate(file_path, regex, parser_fn)",
        before=f"candidate_regex={candidate_regex!r}; full log henüz doğrulanmadı",
    )
    validation = validator.validate(file_path, candidate_regex, parser_fn=parser_fn)
    call_trace(
        "regex_validator.py",
        "RegexValidator.validate(file_path, regex, parser_fn)",
        after=(
            f"accepted={validation.get('accepted')}, "
            f"headers={validation.get('header_matches')}, "
            f"events={validation.get('event_count')}, "
            f"missed={validation.get('candidate_unmatched_headers')}, "
            f"zero_loss={validation.get('accounting_zero_loss')}"
        ),
    )

    kv("valid", validation.get("valid"))
    kv("accepted", validation.get("accepted"))
    kv("coverage_score", validation.get("coverage_score"))
    kv("physical non-empty", validation.get("total_nonempty_lines"))
    kv("regex candidates", validation.get("regex_candidate_matches"))
    kv("header matches", validation.get("header_matches"))
    kv("suppressed matches", validation.get("suppressed_regex_matches"))
    kv("logical event count", validation.get("event_count"))
    kv("missed headers", validation.get("candidate_unmatched_headers"))
    kv("zero loss", validation.get("accounting_zero_loss"))
    pv = validation.get("parser_validation") or {}
    kv("parser attempts", pv.get("attempts"))
    kv("parser success", pv.get("success"))
    kv("parser success %", pv.get("success_ratio"))
    kv("first line sources", validation.get("first_line_sources"))
    kv("classification counts", validation.get("classification_counts"))

    if not validation.get("accepted"):
        print("\n[STOP] Candidate deterministic validation'dan geçmedi.")
        print("Production pipeline bu policy'yi verified olarak kabul etmemelidir.")
        return 4

    if args.summary:
        line_range = None
    else:
        line_range = args.lines or (1, min(total, 50))

    compiled = re.compile(candidate_regex)

    if line_range:
        title("STEP 6", f"LINE-BY-LINE HEADER CLASSIFICATION [{line_range[0]}:{line_range[1]}]")
        print("Burada gerçek HeaderClassifier aynı satırlara tek tek karar veriyor.")
        print("Not: Seçilen aralığın ortasından başlarsak aktif-event bağlamı aralık başında sıfır kabul edilir.")

        has_current_event = False
        after_blank = True

        for line_no, line in read_range(file_path, line_range):
            if not line.strip():
                # Production assembler ile aynı davranış:
                # blank line event'i flush etmez, sadece after_blank context'i yaratır.
                class Dummy:
                    classification = "CONTINUATION"
                    start_new_event = False
                    regex_match = False
                    reason = "blank_context"
                    confidence = 1.0
                call_trace(
                    "multiline_assembler.py",
                    "MultilineAssembler.iter_event_records() [blank handling]",
                    before=f"line={line_no}, after_blank={after_blank}, current_event_active={has_current_event}",
                )
                explain_decision(line_no, line, Dummy(), after_blank)
                after_blank = True
                call_trace(
                    "multiline_assembler.py",
                    "MultilineAssembler.iter_event_records() [blank handling]",
                    after=f"event NOT flushed; after_blank=True, current_event_active={has_current_event}",
                )
                continue

            before_state = (
                f"line={line_no}, raw={line[:160]!r}, "
                f"has_current_event={has_current_event}, after_blank={after_blank}"
            )
            call_trace(
                "header_classifier.py",
                "HeaderClassifier.classify(line, regex, has_current_event, after_blank)",
                before=before_state,
            )
            d = classifier.classify(
                line,
                compiled,
                has_current_event=has_current_event,
                after_blank=after_blank,
            )
            call_trace(
                "header_classifier.py",
                "HeaderClassifier.classify(line, regex, has_current_event, after_blank)",
                after=(
                    f"classification={d.classification}, "
                    f"regex_match={d.regex_match}, "
                    f"start_new_event={d.start_new_event}, "
                    f"reason={d.reason}, confidence={d.confidence}"
                ),
            )
            explain_decision(line_no, line, d, after_blank)

            if d.start_new_event:
                has_current_event = True
            elif not has_current_event:
                # Assembler orphan continuation'ı yine current event olarak toplamaya başlar.
                has_current_event = True
            after_blank = False

        title("STEP 7", f"MULTILINE ASSEMBLY [{line_range[0]}:{line_range[1]} ile kesişen eventler]")
        shown = 0
        call_trace(
            "multiline_assembler.py",
            "MultilineAssembler.iter_event_records(file_path, regex)",
            before=f"physical log + verified regex={candidate_regex!r}",
        )
        for rec in assembler.iter_event_records(file_path, candidate_regex):
            start = int(rec.get("start_line", 0))
            end = int(rec.get("end_line", 0))
            if end < line_range[0] or start > line_range[1]:
                continue

            shown += 1
            call_trace(
                "multiline_assembler.py",
                "MultilineAssembler.iter_event_records() → yield event",
                before=f"physical lines {start}:{end}",
                after=f"1 logical event, line_count={rec.get('line_count')}, source={rec.get('source')}",
            )
            print(f"\nEVENT #{shown}")
            print(SUB)
            kv("Physical range", f"{start}:{end}")
            kv("Line count", rec.get("line_count"))
            kv("First line is header", rec.get("first_line_is_header"))
            kv("Source", rec.get("source"))
            print("EVENT PREVIEW:")
            print(str(rec.get("event", ""))[:1200])

            print("\nDECISIONS:")
            for d in rec.get("decisions", []):
                print(
                    f"  line={d.get('line_no')} "
                    f"class={d.get('classification')} "
                    f"new={d.get('start_new_event')} "
                    f"regex={d.get('regex_match')} "
                    f"reason={d.get('reason')} "
                    f"conf={d.get('confidence')}"
                )

            if shown >= args.event_limit:
                print(f"\n... event-limit={args.event_limit}; kalan eventler gösterilmedi.")
                break

        if shown == 0:
            print("Seçilen aralıkla kesişen logical event bulunamadı.")

    title("STEP 8", "FINAL SEGMENTATION SUMMARY")
    print(
        """
INPUT LOG
   │
   ▼
STRATIFIED SAMPLE
   │
   ▼
FORMAT SIGNATURE
   │
   ├── Registry HIT ───────────────┐
   │                               │
   └── Registry MISS → AI DISCOVERY
                                   │
                                   ▼
                            CANDIDATE REGEX
                                   │
                                   ▼
                          FULL-FILE VALIDATOR
                                   │
                         accepted = True
                                   │
                                   ▼
                          HEADER CLASSIFIER
                    NEW EVENT / CONTINUATION
                                   │
                                   ▼
                         MULTILINE ASSEMBLER
                                   │
                                   ▼
                            LOGICAL EVENTS
"""
    )
    kv("Regex source", candidate_source)
    kv("Regex", candidate_regex)
    kv("Physical lines", total)
    kv("Non-empty lines", nonempty)
    kv("Header matches", validation.get("header_matches"))
    kv("Logical events", validation.get("event_count"))
    kv("Missed headers", validation.get("candidate_unmatched_headers"))
    kv("Zero loss", validation.get("accounting_zero_loss"))
    kv("Parser success %", pv.get("success_ratio"))
    kv("FINAL", "ACCEPTED ✓")

    print("\nÖĞRETİCİ NOT:")
    print("  BEFORE/AFTER, log satırının metninin mutate edildiği anlamına gelmez.")
    print("  BEFORE = fonksiyona giren veri/state; AFTER = fonksiyonun ürettiği karar/çıktı.")
    print("  Assembler aşamasında AFTER gerçekten fiziksel satırların logical event'e dönüşmüş halidir.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
