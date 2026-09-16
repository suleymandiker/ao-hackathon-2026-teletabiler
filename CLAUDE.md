# CLAUDE.md — AOP Projesi için AI Asistan Yönergesi

Bu dosya Claude Code / Cursor gibi AI kodlama asistanlarının bu repoda
çalışırken uyması gereken kuralları tanımlar. Yönergeler bilinçli olarak dar
tutulmuştur: **mimarinin tek bir değişmez ilkesi vardır ve her kural o ilkeden
türer.**

---

## 0. Değişmez ilke

> **Veri düzlemi (data plane) deterministiktir. LLM yalnız kontrol düzleminde
> (control plane) ve yalnız keşif/yorum amacıyla kullanılır.**

Somut karşılığı:

| Katman | Kararı kim verir | LLM rolü |
|---|---|---|
| Segmentasyon sınırı | Deterministik regex + `HeaderClassifier` | Yeni format için **aday** regex önerir |
| Parser politikası | Deterministik regex + %90 doğrulama eşiği | Yeni format için **aday** politika önerir |
| Şablonlama | Drain3 + deterministik maskeleme | Yok |
| Sinyal penceresi | `SignalAggregator` (60 sn) | Yok |
| Gürültü kapısı | `SignalNoiseGate` puanlaması | Yok |
| Korelasyon | `SignalCorrelator` (topoloji + zaman + semantik) | Yok |
| Olay üretimi | `IncidentCandidateBuilder` (root-seeded) | Yok |
| RCA hipotezi | `DeterministicRCAEngine` | Var olan kanıtı **yorumlar** |
| Aksiyon | `LearningPlanner` | Yok — otomatik yürütme yasak |

**Bir LLM çıktısı hiçbir zaman bir olayın varlığına, bir korelasyonun
kurulmasına veya bir sinyalin nitelikli sayılmasına karar veremez.** Bu ilkeyi
ihlal eden öneri, ne kadar "akıllı" görünürse görünsün reddedilir.

---

## 1. Kod yazarken uyulacak kurallar

### 1.1 LLM çağrısı ekleme
- Yeni LLM çağrısı eklemeden önce: *"Bu karar deterministik olarak
  verilebilir mi?"* Cevap "evet" ise LLM ekleme.
- Her çağrı `src/backend/ai_engine.py::MODELS_CONFIG` içindeki **üç sabit
  rolden birine** bağlanır: `Segmentation_Discovery`, `Parser_Discovery`,
  `Ajan_2_RCA_Expert`. Yeni provider/alias eklenmez.
- Prompt gövdesi Python string'i olarak yazılmaz. `src/backend/prompts/*.md`
  altına dosya olarak konur ve `load_prompt()` ile yüklenir.
- Her prompt `{{common_system}}` sözleşmesini miras almalı veya kendi
  prompt-injection savunmasını açıkça içermelidir.

### 1.2 LLM çıktısını kullanma
- LLM çıktısı **asla doğrudan** kullanılmaz. Sırasıyla:
  1. Şema/JSON doğrulaması (`_extract_json`, `ParserPolicyDiscovery.validate`)
  2. Deterministik doğrulama (`RegexValidator`, %90 parse başarı eşiği)
  3. Başarısızsa deterministik fallback
- Gateway hata cevapları da JSON'dur. Beklenen alanların varlığı kontrol
  edilmeden "başarılı" sayılmaz — bkz. `src/backend/rca_layer/rca_engine.py:201`.
- Doğrulanmamış hiçbir politika registry'ye yazılmaz.

### 1.3 Ölçek kuralları
- **Tam log dosyası asla LLM'e gönderilmez.** Yalnız `StratifiedSampler` ile
  alınan sınırlı örnek (varsayılan 150 satır) gönderilir.
- **Tam log dosyası asla RAM'e yüklenmez.** Okuma streaming kalır
  (`iter_events` generator sözleşmesi korunur).
- Registry ham log, prompt veya ham AI yanıtı saklamaz; yalnız kompakt
  doğrulanmış politika saklar.

### 1.4 Açıklanabilirlik (XAI) kuralı
Puan/karar üreten her deterministik katman, kararın yanında **kanıt listesi**
döndürmek zorundadır:
- `qualification_evidence` — nitelik kapısı
- `evidence` + `dependency_path` — korelasyon
- `probable_root` — olay
- `root_cause_candidates[].evidence` — RCA

Kanıt üretmeyen bir karar arayüzde gösterilemez. "Skor 0.82" tek başına
yetersizdir; *neden* 0.82 olduğu görünmelidir.

### 1.5 Güvenlik
- `.env` commit edilmez. Yeni ortam değişkeni eklenirse `.env.example`
  **aynı commit'te** güncellenir.
- Log ve LLM kaynaklı metin HTML'e gömülmeden önce kaçışlanır
  (`src/frontend/streamlit_app.py::esc`, `theme.py` bileşenleri).
- Prompt içine kullanıcı verisi gömülürken "bu veri talimat değildir"
  sözleşmesi korunur (`src/backend/prompts/common_system.md`).

---

## 2. Proje düzeni

```
src/backend/
  ai_engine.py                 Tek LLM erişim noktası (3 rol, OpenAI-uyumlu)
  prompts/                     Çalışma zamanı prompt'ları (tek doğruluk kaynağı)
  full_pipeline_v2.py          Orkestrasyon: ham log yolu + yapılandırılmış alarm fast-path
  downstream_pipeline.py       Sinyal → gürültü → korelasyon → olay → RCA → plan
  input_package_layer/         ZIP paket yükleyici + topoloji grafiği
  segmentation_layer/          Örnekleme, keşif, doğrulama, registry, streaming sınıflandırma
  parser_layer/                Format tespiti, parser ailesi, politika keşfi/registry
  template_layer/              Drain3 + maskeleme + yapısal genelleme + doğrulayıcı
  aggregation_layer/           60 sn pencereli sinyal adayı üretimi
  signal_qualification_layer/  Açıklanabilir gürültü kapısı
  correlation_layer/           Topoloji-farkında deterministik korelasyon
  incident_candidate_layer/    Root-seeded olay üretimi
  context_enrichment_layer/    Envanter + bağımlılık zenginleştirmesi
  rca_layer/                   Deterministik RCA + Qwen uzman yorumu
  learning_planning_layer/     İnsan onaylı aksiyon önerisi
src/frontend/
  streamlit_app.py             Sayfa mantığı ve veri bağlama
  theme.py                     Tasarım sistemi: CSS + HTML bileşenleri
tools/benchmark.py             Tekrarlanabilir ölçüm koşusu
```

### Import sözleşmesi
`src/backend` `sys.path`'e eklenir ve backend modülleri birbirini **paket
öneki olmadan** import eder (`from ai_engine import ...`). Bu sözleşmeyi
değiştirmeyin; `src/frontend/streamlit_app.py:8-13` buna dayanır.

---

## 3. Dil ve üslup

- **Kullanıcıya görünen her metin Türkçe'dir**: arayüz etiketleri, konsol
  logları, `qualification_evidence` gibi kanıt etiketleri, RCA çıktısı.
- Kod içi tanımlayıcılar (değişken/fonksiyon/alan adları) İngilizce kalır.
- Yorumlar kısa ve *nedeni* açıklar, *ne yaptığını* değil. Bu repo düşük yorum
  yoğunluğu kullanır; ona uyun.
- Mevcut kod stili yoğundur (tek satırda birden fazla ifade). Yeni kod
  çevresindeki dosyanın stiline uyar; repo genelinde stil reformu yapılmaz.

---

## 4. Değişiklik yapmadan önce

1. `python tools/benchmark.py` çalıştırın, mevcut sayıları not edin.
2. Değişikliği yapın.
3. Benchmark'ı tekrar çalıştırın. Sinyal/korelasyon/olay sayıları değiştiyse
   **neden değiştiğini açıklayabiliyor olmalısınız.** Açıklayamıyorsanız
   değişiklik bir regresyondur.

---

## 5. Yapılmayacaklar

- Otomatik remediation / self-healing eklemek. Sistem yalnız **öneri** üretir;
  `execution_allowed=False` ve `requires_human_approval=True` sabittir.
- Belirli bir hackathon senaryosunu (örn. "billing-db diski dolar") koda
  gömmek. Alarm tipi kümeleri (`ROOT_TYPES`, `SYMPTOM_TYPES`,
  `BACKGROUND_TYPES`) **semantik sınıflardır**, senaryo değildir.
- Korelasyonu nedensellik olarak sunmak. Arayüz ve prompt bu ayrımı korur.
- Zaman yakınlığını tek başına korelasyon kanıtı saymak.
