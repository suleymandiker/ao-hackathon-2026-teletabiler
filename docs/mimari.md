# Mimari — AOP (Ajan Tabanlı Operasyon Platformu)

## 0. Tek cümlelik mimari

Ham log / alarm akışını **9 deterministik katmandan** geçirerek gürültüden
arındırılmış, topoloji-farkında, her adımı kanıtla açıklanabilir bir olay
hipotezine dönüştüren; LLM'i yalnız **format keşfi** ve **uzman yorumu** için
kullanan bir SRE zekâ hattı.

---

## 1. Kontrol düzlemi / veri düzlemi ayrımı

Bu projenin tek mimari ilkesi budur:

```
┌────────────────────── KONTROL DÜZLEMİ (seyrek, AI'lı) ─────────────────────────┐
│                                                                                │
│  Bilinmeyen format?  ──►  DeepSeek keşif  ──►  deterministik doğrulama         │
│                                                     │                          │
│                                         ┌───────────┴───────────┐              │
│                                      GEÇTİ                   KALDI             │
│                                         │                       │              │
│                              registry'ye yaz          fallback politika        │
│                                                                                │
│  Olay üretildi?      ──►  Qwen uzman yorumu  ──►  şema doğrulama  ──► göster   │
│                           (karar üretmez)      başarısız ise: deterministik    │
└────────────────────────────────────────────────────────────────────────────────┘
        │ politika (regex + alan eşlemesi)
        ▼
┌────────────────────── VERİ DÜZLEMİ (sıcak yol, %100 deterministik) ────────────┐
│  streaming okuma → sınıflandırma → parse → şablon → pencere → kapı →           │
│  korelasyon → olay → RCA hipotezi → plan                                       │
│  Bu yolda tek bir LLM çağrısı yoktur.                                          │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Neden:** Bir SRE aracı, LLM'in o anki ruh haline göre farklı olay üretemez.
Aynı girdi → aynı olay → aynı kanıt. LLM sadece (a) daha önce görülmemiş bir
log formatını çözmek ve (b) hazır kanıtı operatör diline çevirmek için vardır.

Kod karşılığı:
- LLM erişiminin **tek** noktası: [ai_engine.py:45-64](../src/backend/ai_engine.py#L45-L64) — üç sabit rol, başka provider/alias yok.
- Deterministik downstream orkestrasyonu: [downstream_pipeline.py:29-46](../src/backend/downstream_pipeline.py#L29-L46) — bu dosyada `ai_engine` import'u yoktur.

---

## 2. Uçtan uca akış

```
                     ┌──────────────────────────────────────────┐
  ZIP paketi ───────►│ 0. Input Package Layer                   │
  (alarms.json +     │    3 dosyayı ayrıştır, envanterle        │
   host_inventory +  │    çapraz doğrula, TopologyContext kur   │
   service_deps)     └──────────────────┬───────────────────────┘
                                        │
  Ham log dosyası ──────────────────────┤
  (.log/.txt/.md)                       │
                                        ▼
        ┌───────────────────────────────────────────────────────┐
        │ 1. Segmentation   sınırlı örnek → imza → registry     │
        │    miss ise DeepSeek → doğrula → sakla → streaming    │
        │    çok satırlı kayıt (traceback) = 1 mantıksal olay   │
        └──────────────────────────┬────────────────────────────┘
                                   ▼
        ┌───────────────────────────────────────────────────────┐
        │ 2. Parser         format tespiti → CanonicalEvent     │
        │    json/kv/syslog/structured/positional/plain/policy  │
        └──────────────────────────┬────────────────────────────┘
                                   ▼
        ┌───────────────────────────────────────────────────────┐
        │ 3. Template       Drain3 + maskeleme + yapısal genelle│
        │    → template_id (tekrar eden log davranışı)          │
        └──────────────────────────┬────────────────────────────┘
                                   ▼
        ┌───────────────────────────────────────────────────────┐
        │ 4. Aggregation    60 sn pencere × (alarm_type,        │
        │    template_id, servis) → sinyal adayı                │
        └──────────────────────────┬────────────────────────────┘
                                   ▼
        ┌───────────────────────────────────────────────────────┐
        │ 5. Qualification  açıklanabilir gürültü kapısı        │
        │    puan + kanıt listesi; eşik 0.60                    │
        └──────────────────────────┬────────────────────────────┘
                                   ▼
        ┌───────────────────────────────────────────────────────┐
        │ 6. Correlation    topoloji yönü + zaman sırası +      │
        │    alarm semantiği (DC/kabin dahil)                   │
        └──────────────────────────┬────────────────────────────┘
                                   ▼
        ┌───────────────────────────────────────────────────────┐
        │ 7. Incident       root-seeded kümeleme                │
        │    + Context Enrichment (envanter, bağımlılık yolu)   │
        └──────────────────────────┬────────────────────────────┘
                                   ▼
        ┌───────────────────────────────────────────────────────┐
        │ 8. RCA            deterministik hipotez sıralaması    │
        │    + tek Qwen uzman case yorumu (opsiyonel)           │
        └──────────────────────────┬────────────────────────────┘
                                   ▼
        ┌───────────────────────────────────────────────────────┐
        │ 9. Planning       sahip + sonraki güvenli adım        │
        │    execution_allowed = False (her zaman)              │
        └───────────────────────────────────────────────────────┘
```

Orkestrasyon: [full_pipeline_v2.py](../src/backend/full_pipeline_v2.py) ve
[downstream_pipeline.py](../src/backend/downstream_pipeline.py).

### İki giriş yolu, tek downstream

`FullAIOpsPipelineV2` iki farklı girdi sınıfını ayırır:

| Yol | Metot | Neden ayrı |
|---|---|---|
| Yapılandırılmış alarm paketi (ZIP/JSON/CSV) | `process_package` → `process_structured_alarms` ([full_pipeline_v2.py:30-80](../src/backend/full_pipeline_v2.py#L30-L80)) | Her satır zaten bir kayıt. Çok satırlı log segmentasyonundan geçirmek kayıp/yanlış birleşme riski yaratır. Kimlik `service + alarm_type` üzerinden deterministik kurulur, **kayıp = 0**. |
| Ham log akışı (.log/.txt/.md) | `process_file` ([full_pipeline_v2.py:82-114](../src/backend/full_pipeline_v2.py#L82-L114)) | Format bilinmiyor, kayıtlar çok satırlı olabilir. Tam segmentasyon + parser + Drain3 zinciri çalışır. |

Her iki yol da aynı `DownstreamAIOpsPipeline`'a bağlanır — sinyal sonrası
mantık tek ve ortaktır.

---

## 3. Katman katman tasarım kararları

### Katman 0 — Input Package Layer
[src/backend/input_package_layer/package_loader.py](../src/backend/input_package_layer/package_loader.py)

ZIP içindeki üç dosya **isimle değil, kolon imzasıyla** tanınır
([package_loader.py:22-33](../src/backend/input_package_layer/package_loader.py#L22-L33)).
Bu sayede `alarms.json` / `alarmlar.json` / `veri/alarms.json` fark etmez.

Yükleme sırasında alarm ↔ envanter **çapraz doğrulaması** yapılır ve veri
kalitesi sayılır: `inventory_missing`, `service_mismatch`, `dc_mismatch`,
`rack_mismatch` ([package_loader.py:44-48](../src/backend/input_package_layer/package_loader.py#L44-L48)).
Arayüz bunu uyarı kartı olarak gösterir — "veriye körü körüne güvenilmiyor"
kanıtı buradadır.

`TopologyContext` ([topology.py](../src/backend/input_package_layer/topology.py)) iki yönlü
bağımlılık grafiğini kurar ve `relation(a, b)` ile iki servis arasındaki
**yönlü nedensel ilişkiyi** (kim kime bağımlı, kaç hop) döndürür.

### Katman 1 — Segmentation
[src/backend/segmentation_layer/](../src/backend/segmentation_layer/)

Problem: 100 GB'lık bir log dosyasında bir Java stack trace veya Python
traceback **tek bir mantıksal olaydır**, 12 satır değil. Ama hangi satırın
"yeni olay başlangıcı" olduğunu bilmeden bunu ayıramazsınız.

Çözüm zinciri:

```
StratifiedSampler      dosyanın tamamını değil, bayt aralığına yayılmış
(≤150 satır)           sınırlı pencereleri okur; küçük dosya tam okunur
        │
        ▼
_format_signature      örnekten yapısal imza (hash) üretir
        │
        ▼
SegmentationPolicyRegistry   RAM dict → SQLite
        │
   ┌────┴────┐
 HIT        MISS
   │          │
   │          ▼
   │   DeepSeek keşfi (yalnız örnek gönderilir)
   │          │
   │          ▼
   │   RegexValidator — TÜM akışta deterministik doğrulama
   │   (coverage + parser başarı oranı)
   │          │
   │     ┌────┴────┐
   │  GEÇTİ     KALDI
   │     │         │
   │     │    DEFAULT_FALLBACK_REGEX doğrulanır, gerekirse o saklanır
   │     ▼
   │  registry'ye yaz
   └─────┬─────┘
         ▼
HeaderClassifier + MultilineAssembler   (streaming, AI = 0)
         ▼
   mantıksal olaylar (generator)
```

Kritik detay: **imza örneklemesi ile keşif örneklemesi ayrıdır**
([segmentation_pipeline.py:136-141](../src/backend/segmentation_layer/segmentation_pipeline.py#L136-L141)).
Stratified örnekleme bayt pozisyonuna dayandığı için dosya büyüdükçe örnek
kayar; imza için bu kullanılsaydı her eklenen satır "yeni format" sayılır ve
gereksiz AI çağrısı doğururdu.

Registry yalnız **kompakt doğrulanmış politika** saklar: ham log, prompt veya
ham AI yanıtı saklanmaz.

### Katman 2 — Parser
[src/backend/parser_layer/](../src/backend/parser_layer/)

`FormatDetector` mantıksal olayı bir parser ailesine yönlendirir: JSON, KV,
syslog, structured-text, positional-structured, plain-text. Hiçbiri uymuyorsa
`ParserPolicyDiscovery` devreye girer: DeepSeek bir parser politikası
(regex + grup eşlemesi) önerir, deterministik doğrulayıcı bunu örneklerin
**≥ %90'ında** çalıştırmayı zorunlu tutar
([policy_discovery.py:422](../src/backend/parser_layer/policy/policy_discovery.py#L422)).
Geçemezse politika atılır.

Çıktı tek tip: `CanonicalEvent` — `timestamp`, `severity_text/number`,
`message`, `service_name`, `host`, `resource`, `attributes`. Downstream
katmanların hiçbiri log formatını bilmez.

Timestamp'ler `timestamp_normalizer` + `timestamp_evidence` ile normalize
edilir; yıl/timezone bilgisi olmayan damgalar bilinçli olarak `null`
bırakılır ("uydurmaktansa boş bırak").

### Katman 3 — Template
[src/backend/template_layer/](../src/backend/template_layer/)

Drain3 aday şablonu üretir; `masker` değişken parçaları (ID, sayı, IP, UUID)
maskeler; `structural_generalizer` yapısal genelleme yapar; `validator`
şablonun **güvenilir** olup olmadığına karar verir (`template_reliable`).
Güvenilmez şablonlar atılmaz — işaretlenir ve downstream'de `reliable_ratio`
olarak puanı düşürür.

### Katman 4 — Aggregation
[src/backend/aggregation_layer/aggregator.py](../src/backend/aggregation_layer/aggregator.py)

60 saniyelik pencerelerde gruplama anahtarı:
`(alarm_type or template_id, template_id, kimlik, pencere)`
([aggregator.py:117-122](../src/backend/aggregation_layer/aggregator.py#L117-L122)).

`alarm_type`'ın anahtara dahil edilmesi bilinçlidir: aynı servisin aynı
penceredeki **ilgisiz semptomları** (örn. `timeout` ve `cert_expiry`) tek
sinyalde çökmesin.

Servis/bileşen bilinmiyorsa log metninden desenle çıkarım denenir
([aggregator.py:28-49](../src/backend/aggregation_layer/aggregator.py#L28-L49)) — ham
loglarda `service_name` alanı çoğu zaman yoktur.

### Katman 5 — Signal Qualification (gürültü kapısı)
[src/backend/signal_qualification_layer/noise_gate.py](../src/backend/signal_qualification_layer/noise_gate.py)

Alarm tipleri üç semantik sınıfa ayrılır:

| Sınıf | Örnek | Puan etkisi |
|---|---|---|
| `ROOT_TYPES` | `disk_full`, `db_write_fail`, `network_down`, `oom_risk` | **+0.48** |
| `SYMPTOM_TYPES` | `timeout`, `http_5xx`, `latency_high`, `queue_backlog` | **+0.32** |
| `BACKGROUND_TYPES` | `cert_expiry`, `backup_warn`, `ntp_drift`, `cpu_high` | **−0.20** |

Üstüne: önem seviyesi, tekrar/burst, açık hata semantiği, şablon
güvenilirliği. Eşik 0.60.

Arka plan telemetrisi için **ayrı ve daha sert bir kural** vardır
([noise_gate.py:44-46](../src/backend/signal_qualification_layer/noise_gate.py#L44-L46)):
tek bir yüksek-severity satır yüzünden `cpu_high` gürültüsü geçemez;
`count>=10 AND source_severity>=4` şartı aranır.

Her sinyal kararının yanında `qualification_evidence` listesi döner —
arayüzde "neden bastırıldı / neden geçti" doğrudan okunur.

### Katman 6 — Correlation
[src/backend/correlation_layer/correlator.py](../src/backend/correlation_layer/correlator.py)

Dört kanıt ailesi; en güçlüsü kazanır:

| Kanıt | Skor | Koşul |
|---|---|---|
| Aynı servis, zaman yakınlığı | 0.58–0.68 | ≤180 sn, uyumlu alarm ailesi |
| Log içi bağımlılık referansı | 0.78 | A'nın servis adı B'nin metninde geçiyor |
| **Servis bağımlılığı (topoloji)** | **0.66 / 0.74 / 0.82** | 3 / 2 / 1 hop |
| Aynı DC + aynı kabin + ağ alarm ailesi | 0.88 | ≤300 sn |

Topoloji kenarının kabul edilmesi için **üç şartın birlikte** sağlanması
gerekir ([correlator.py:53-57](../src/backend/correlation_layer/correlator.py#L53-L57)):
1. `causal_compatible` — kök tarafta gerçek bir kök alarm ailesi ya da bağımlı
   tarafta yayılan bir semptom olmalı
2. `temporal` — zaman farkı pencere içinde
3. `root_first` — kök, bağımlıdan **önce** başlamış olmalı

Yani "iki servis aynı anda alarm verdi" tek başına korelasyon değildir. Kenar
yönü daima **kök → bağımlı**'dır ve `dependency_path` kanıt olarak taşınır.

### Katman 7 — Incident Candidate + Context Enrichment
[src/backend/incident_candidate_layer/builder.py](../src/backend/incident_candidate_layer/builder.py)

Naif yaklaşım korelasyon grafiğinin bağlı bileşenlerini almaktır. Bunun bilinen
hatası: iki **bağımsız** kök neden, ortak bir semptom üzerinden köprülenip tek
dev olaya çöker.

Bu repo bunu **root-seeded** kümeleme ile çözer:
1. Yalnız `ROOT_TYPES` sinyalleri küme çekirdeği olabilir.
2. Çekirdekler yalnız birbirleriyle birleşebilir (`_root_related`: aynı servis
   + aynı alarm ailesi, ya da aynı DC+kabin ağ patlaması, ya da topolojik
   olarak bağlı DB ailesi).
3. Semptomlar **en iyi tek çekirdeğe** iliştirilir; asla iki çekirdeği
   birleştiremez ([builder.py:62-72](../src/backend/incident_candidate_layer/builder.py#L62-L72)).

Kök varlık seçimi: ağ ailesi çok servisli ve tek DC+kabin ise kök bir
**altyapı** varlığıdır (`DC/kabin`), tek servis değil
([builder.py:83-84](../src/backend/incident_candidate_layer/builder.py#L83-L84)).
DB kümelerinde ise bağımlılığın **hedefi** tercih edilir (örn. `billing-api`
değil `billing-db`)
([builder.py:88-91](../src/backend/incident_candidate_layer/builder.py#L88-L91)).

Kök tipi hiç yoksa eski bağlı-bileşen davranışına düşülür — ham log
senaryoları için gerekli
([builder.py:26-45](../src/backend/incident_candidate_layer/builder.py#L26-L45)).

`ContextEnricher` olaya envanter satırlarını, DC/kabin/iş kritikliği bilgisini
ve servisler arası **bağımlılık yollarını** ekler.

### Katman 8 — RCA
[src/backend/rca_layer/rca_engine.py](../src/backend/rca_layer/rca_engine.py)

İki aşamalı:

1. `DeterministicRCAEngine` — olay içi sinyalleri (önem, ilk görülme, giden
   korelasyon sayısı, nitelik puanı) ile sıralar, ilk 5'i kanıtlı hipotez
   olarak döndürür. **LLM olmadan da çalışır.**
2. `ExpertRCAEngine` — tüm case için **tek** Qwen çağrısı yapar. Qwen'e giden
   yük kompakt ve sınırlıdır: ≤5 olay, ≤20 sinyal, ≤30 korelasyon kenarı
   ([rca_engine.py:145-183](../src/backend/rca_layer/rca_engine.py#L145-L183)).

Qwen çıktısı iki kapıdan geçer:
- JSON çıkarımı (kod çitleri temizlenir, gövde içinden nesne kurtarılır)
- **Semantik kapı**: gateway hata cevapları da geçerli JSON'dur. Beklenen
  alanlardan (`durum_ozeti`, `kok_neden_hipotezi`, `guven`,
  `nedensellik_durumu`) hiçbiri yoksa çıktı reddedilir
  ([rca_engine.py:201](../src/backend/rca_layer/rca_engine.py#L201)).

Reddedilirse `last_ai_error` doldurulur ve arayüz "Qwen alınamadı —
deterministik RCA korunuyor" rozetini gösterir. **Deterministik sonuç hiçbir
koşulda kaybolmaz.**

### Katman 9 — Planning
[src/backend/learning_planning_layer/planner.py](../src/backend/learning_planning_layer/planner.py)

Kök varlığın şekline göre sahip ataması (`-db` → Database Operations,
`-gw`/`provider` → Integration/Network, altyapı → Network Operations) ve üç
güvenli sonraki adımdan biri. `execution_allowed=False` ve
`requires_human_approval=True` sabittir — sistem hiçbir şeyi kendi uygulamaz.

---

## 4. Frontend mimarisi

| Dosya | Rol |
|---|---|
| [.streamlit/config.toml](../.streamlit/config.toml) | Turkcell tema token'ları (sarı `#FFC800` / lacivert `#0A1628`) |
| [src/frontend/theme.py](../src/frontend/theme.py) | Tasarım sistemi: global CSS + `hero`, `kpi`, `flow`, `trace_card`, `meter`, `chips` bileşenleri |
| [src/frontend/streamlit_app.py](../src/frontend/streamlit_app.py) | Sayfa mantığı ve veri bağlama |

İki görünüm:
- **Operasyon Analizi** — KPI'lar, sinyal tablosu, olay listesi ve olay başına
  5 sekmeli XAI incelemesi (uzman yorumu / hipotezler / korelasyon kanıtı /
  sinyal kanıtları / aksiyon).
- **Katman İzleme** — 9 katmanın her biri için *Girdi → Uygulanan Karar →
  Çıktı* kartları ve o katmanın gerçek pipeline çıktısı. Jürinin "bu sayı
  nereden geldi" sorusunun cevabı buradadır.

Güvenlik: log ve LLM kaynaklı tüm metin HTML kartlara gömülmeden önce `esc()`
ile kaçışlanır
([streamlit_app.py:111-113](../src/frontend/streamlit_app.py#L111-L113)).

---

## 5. Kalıcılık

Tek kalıcı durum `data/policy_registry.sqlite3`'tür (ortam değişkeni
`AIOPS_POLICY_REGISTRY_PATH` ile taşınabilir). İçinde yalnız doğrulanmış
segmentasyon ve parser politikaları bulunur.

| Saklanan | Saklanmayan |
|---|---|
| Format imzası (hash) | Ham log satırları |
| Doğrulanmış regex + alan eşlemesi | Prompt metinleri |
| Doğrulama metrikleri | Ham LLM yanıtları |
| Şema sürümü | Müşteri / PII verisi |

Şablon durumu ayrıca `data/template_state_v4f.json` + `template_drain_v4f.bin`
olarak yazılır (yalnız ham log yolunda).

---

## 6. Hata dayanıklılığı

| Arıza | Davranış |
|---|---|
| `SAKA_API_KEY` yok | AI çağrısı yapılmaz; segmentasyon fallback regex'e, RCA deterministik motora düşer |
| Gateway 4xx/5xx | Hata metni yakalanır, arayüzde uyarı rozeti; pipeline devam eder |
| Gateway `response_format` desteklemiyor | Alan çıkarılıp istek bir kez sade olarak tekrarlanır ([ai_engine.py:152-156](../src/backend/ai_engine.py#L152-L156)) |
| AI regex doğrulamayı geçemiyor | Fallback regex doğrulanır ve gerekirse registry'ye o yazılır |
| Qwen bozuk/alakasız JSON döndürdü | Semantik kapı reddeder; deterministik RCA gösterilir |
| ZIP'te beklenen kolonlar yok | Açık Türkçe hata mesajı ile durdurulur (sessiz yanlış sonuç üretilmez) |

---

## 7. Bilinen mimari sınırlar

- Korelasyon penceresi O(n²) tarama yapar; zaman sırası + erken `break` ile
  pratikte doğrusala yakındır, ancak çok geniş pencerelerde maliyet artar.
- `TopologyContext` statiktir; paket içindeki bağımlılık dosyası neyse odur.
  Çalışma zamanı servis keşfi (service mesh, distributed trace) entegre
  edilmemiştir.
- "Learning" katmanı şu an yalnız planlama üretir; operatör geri bildirimini
  registry'ye geri besleyen döngü kurulmamıştır.
- Şablon durumu ham log yolunda diske yazılır; çok işlemli eşzamanlı yazım
  için kilitleme yoktur.
