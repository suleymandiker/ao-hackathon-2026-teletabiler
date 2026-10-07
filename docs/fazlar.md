# Fazlar — Geliştirme Süreci ve Ölçümler

Bu belge projenin hangi aşamalardan geçtiğini, her aşamada **neyi yanlış
yaptığımızı**, nasıl düzelttiğimizi ve son ölçülen sayıları kaydeder.

> **Dürüstlük notu:** Bu repo'nun commit geçmişi (4 commit) geliştirme sürecini
> yansıtmıyor — çalışmanın büyük kısmı tek çalışma alanında yapılıp toplu
> push edildi. Bu bir eksiktir. Aşağıdaki fazların kanıtı bu yüzden commit
> mesajlarında değil, **kodda kalan izlerde** aranmalıdır: silinen tasarımların
> notu ([SEGMENTATION_FINAL.md](../SEGMENTATION_FINAL.md)), fallback yolları,
> savunma kontrolleri ve `tools/benchmark.py` ile tekrar üretilebilen sayılar.

---

## Faz 1 — Segmentasyon: "çok satırlı log tek olaydır"

**Amaç:** 100 GB'lık bir log dosyasını RAM'e yüklemeden ve LLM'e göndermeden
mantıksal olaylara bölmek. Bir Python traceback'i 8 satır değil, 1 olaydır.

**İlk deneme ve hatası:**
Basit `^timestamp` regex'i. Traceback'lerin `File "..."` satırları başlık
sanılmadı ama girintisiz `TimeoutError: ...` satırı sorun çıkardı. Ayrıca her
yeni log formatı için elle regex yazmak ölçeklenmedi.

**Çözüm:**
`StratifiedSampler` → yapısal imza → politika registry → (miss ise) DeepSeek
keşfi → `RegexValidator` ile **tüm akışta** doğrulama → registry'ye yazma.
Üstüne `HeaderClassifier` continuation koruması (traceback, Java stack,
girintili blok, iliştirilmiş JSON).

**Aşırı mühendislikten geri dönüş (kayıtlı):**
[SEGMENTATION_FINAL.md](../SEGMENTATION_FINAL.md) bu fazda **silinen**
tasarımları listeliyor:
- `segmentation_cache.json` / `SegmentationCache` (registry ile çakışan ikinci
  doğruluk kaynağı)
- global `.aiops_ai_cache.sqlite3`
- segmentasyon refinement/critic aşamaları (AI'a AI ile bakma)
- drift/revalidation durum makinesi

Bu silmeler bilinçli bir sadeleştirme kararıdır: iki doğruluk kaynağı = bug
kaynağı. Politika durumu `data/policy/policy_registry.sqlite3` altında tutulur.

**Ölçüm:** 246 fiziksel satır → **44 mantıksal olay**, coverage %100,
parser başarısı %100.

---

## Faz 2 — Parser ve şablonlama: format bağımsızlığı

**Amaç:** Downstream katmanların log formatını hiç bilmemesi.

**Yapılan:** `FormatDetector` + 7 parser ailesi (JSON, KV, syslog,
structured-text, positional, plain, policy). Bilinmeyen format için
`ParserPolicyDiscovery`: DeepSeek bir parser politikası önerir, doğrulayıcı
örneklerin **≥%90'ında** çalışmasını şart koşar.

**Kritik karar — "uydurmaktansa boş bırak":**
Yıl/timezone bilgisi olmayan bir timestamp deterministik olarak `null`
bırakılır. Model "2026" tahmin etse bile kabul edilmez. Bu karar
[parser_discovery.md](../src/backend/prompts/parser_discovery.md) prompt'unda
da açıkça yazılıdır.

**Şablonlama:** Drain3 + maskeleme + yapısal genelleme + doğrulayıcı.
Güvenilmez şablon atılmaz, `template_reliable=False` ile işaretlenir ve
downstream'de puanı düşürür.

**Ölçüm:** 44 olay → 44 canonical event → 44 şablonlanmış olay, kayıp 0;
`template_unreliable=4` (işaretlendi, atılmadı).

---

## Faz 3 — Yapılandırılmış alarm paketi için ayrı yol

**Problem:** Hackathon paketindeki `alarms.json` satır başına bir JSON
nesnesidir. Bunu çok satırlı log segmentasyonundan geçirmek yanlış birleşme
ve kayıp riski yaratıyordu.

**Çözüm:** `process_structured_alarms` fast-path
([full_pipeline_v2.py:30-80](../src/backend/full_pipeline_v2.py#L30-L80)).
Kimlik `service + alarm_type` üzerinden deterministik kurulur; her kaynak
alarm tam olarak bir canonical/template event olur.

**Ayrıca:** `InputPackageLoader` üç dosyayı **isimle değil kolon imzasıyla**
tanır ve alarm ↔ envanter çapraz doğrulaması yapar. Uyuşmazlıklar sessizce
yutulmaz, arayüzde uyarı olarak gösterilir.

**Ölçüm:** 91 alarm → 91 event, `kayıp=0`.

---

## Faz 4 — Gürültü kapısı: "neden bastırıldı" görünmeli

**İlk deneme ve hatası:** Sadece severity eşiği. `cpu_high` alarmlarından biri
yüksek severity ile geldiğinde 30 satırlık arka plan gürültüsü kapıdan geçti.

**Çözüm:** Alarm tipleri üç semantik sınıfa ayrıldı
(`ROOT_TYPES` +0.48 / `SYMPTOM_TYPES` +0.32 / `BACKGROUND_TYPES` −0.20) ve
arka plan telemetrisi için **ayrı, daha sert bir kural** kondu:
`count>=10 AND source_severity>=4`
([noise_gate.py:44-46](../src/backend/signal_qualification_layer/noise_gate.py#L44-L46)).

Her karar `qualification_evidence` listesiyle birlikte döner. Arayüzde
"GEÇTİ / BASTIRILDI" kararının yanında kanıt etiketleri görünür.

**Ölçüm (S1):** 15 sinyal adayının 8'i bastırıldı = **%53.3 gürültü azaltma**.
`cpu_high`, `backup_warn`, `ntp_drift` ailelerinin tamamı bastırıldı.

---

## Faz 5 — Korelasyon: zamandan nedenselliğe

**İlk deneme ve hatası:** Zaman yakınlığı + aynı servis. Sonuç: aynı saniyede
patlayan ilgisiz alarmlar birbirine bağlandı. Korelasyon nedensellik gibi
sunuldu.

**Çözüm:** `service_dependencies.csv`'den `TopologyContext` kuruldu ve
topolojik kenar için **üç şart birden** zorunlu hale getirildi
([correlator.py:53-57](../src/backend/correlation_layer/correlator.py#L53-L57)):
1. `causal_compatible` — kökte gerçek kök alarm ailesi ya da bağımlıda yayılan
   semptom
2. `temporal` — zaman farkı pencere içinde
3. `root_first` — kök, bağımlıdan **önce** başlamış

Kenar yönü daima kök → bağımlı. `dependency_path` kanıt olarak taşınır.
Ayrıca fiziksel kanıt eklendi: aynı DC + aynı kabin + ağ alarm ailesi = 0.88.

**Ölçüm (S1):** 19 korelasyon kenarı; hepsi yönlü ve kanıtlı. Örnek:
`billing-db → billing-api` skor 0.82, kanıt
`['servis_bağımlılığı', '1_hop', 'nedensel_zaman_sırası', 'kök_alarm_tipi']`.

---

## Faz 6 — Olay üretimi: bağlı bileşenden root-seeded'e

**En önemli düzeltme.** İlk sürüm korelasyon grafiğinin **bağlı
bileşenlerini** olay sayıyordu. Sonuç felaketti: iki bağımsız kök neden, ortak
bir semptom (örn. her ikisinin de `timeout` üretmesi) üzerinden köprülenip tek
dev olaya çöküyordu. Operatör "tek bir olay var" sanıyordu.

**Çözüm — root-seeded kümeleme**
([builder.py:46-99](../src/backend/incident_candidate_layer/builder.py#L46-L99)):
1. Yalnız `ROOT_TYPES` sinyalleri küme **çekirdeği** olabilir.
2. Çekirdekler yalnız birbirleriyle birleşir (`_root_related`).
3. Semptomlar **en iyi tek çekirdeğe** iliştirilir, asla iki çekirdeği
   birleştiremez.

Ayrıca kök varlık seçimi akıllandırıldı:
- Çok servisli ağ arızası tek DC+kabin'de ise kök bir **altyapı** varlığıdır
  (`DC1/R12`), tek bir servis değil.
- DB kümesinde bağımlılığın **hedefi** tercih edilir: `billing-api` değil
  `billing-db`.

Kök tipi hiç yoksa (ham log senaryosu) eski bağlı-bileşen davranışı fallback
olarak korundu.

**Ölçüm (S1):** 4 servislik zincir (`billing-db → billing-api → payment-api →
order-gw`) **1 olayda** toplandı; kök varlık `billing-db`, kök alarm
`disk_full`, güven 0.85. Bağımsız arka plan gürültüsü bu olaya karışmadı.

---

## Faz 7 — RCA: deterministik hipotez + tek uzman yorumu

**Reddedilen yaklaşım:** Her olay için ayrı LLM çağrısı. Maliyet olay sayısıyla
doğrusal artıyordu ve model olaylar arası ilişkiyi göremiyordu.

**Çözüm:** Tüm case için **tek** Qwen çağrısı; yük kompakt ve sınırlı
(≤5 olay, ≤20 sinyal, ≤30 kenar).

**Yakalanan gerçek bug:** Gateway hata cevapları da geçerli JSON'dur.
`_extract_json` bunları "başarılı RCA" olarak kabul ediyor ve arayüzde hata
gövdesi uzman yorumu gibi görünüyordu. Semantik kapı eklendi
([rca_engine.py:201](../src/backend/rca_layer/rca_engine.py#L201)): beklenen
alanlardan (`durum_ozeti`, `kok_neden_hipotezi`, `guven`,
`nedensellik_durumu`) hiçbiri yoksa çıktı reddedilir.

**Ölçüm (S2):** 1 Qwen çağrısı, 5.4–5.7 sn, ~7.1k token.
Uzman güveni 0.95, nedensellik `destekleniyor`, hipotez:
*"billing-db sunucusunun (db-01) veri hacminin %98 doluluk oranına ulaşması
nedeniyle disk alanının tükenmesi."* — deterministik `probable_root` ile
birebir uyumlu.

---

## Faz 8 — Arayüz: XAI öncelikli çalışma alanı

**İlk sürüm:** Sonuç JSON'unu tablolar halinde döken bir panel. Sayılar
vardı, gerekçe yoktu.

**V2 (mevcut):** İki görünüm.
- **Operasyon Analizi** — KPI'lar, sinyal tablosu, olay listesi ve olay başına
  5 sekmeli XAI incelemesi.
- **Katman İzleme** — 9 katmanın her biri için *Girdi → Uygulanan Karar →
  Çıktı* kartları ve o katmanın **gerçek** pipeline çıktısı.

Tasarım sistemi `theme.py` içinde tek yerde toplandı (Turkcell sarısı
`#FFC800` / lacivert `#0A1628`). Tüm bileşenler HTML kaçışı uygular; log ve
LLM kaynaklı metin kartlara güvenle gömülür.

Detay: [STREAMLIT_UI.md](../STREAMLIT_UI.md).

---

## Faz 9 — Ölçüm ve belgeleme

`tools/benchmark.py` eklendi: sentetik (rastgelelik içermeyen) bir alarm
paketi üretir, dört senaryoyu koşar, tabloyu ve **7 doğruluk kontrolünü**
yazdırır. Bu belgedeki ve [AI_JURI.md](../AI_JURI.md)'deki her sayı tek
komutla yeniden üretilebilir.

---

## Son ölçüm tablosu

`python tools/benchmark.py` · Windows 11 · Python 3.10 · 2026-09-16

| Senaryo | sn | olay | sinyal adayı | nitelikli | gürültü | korelasyon | incident |
|---|---:|---:|---:|---:|---:|---:|---:|
| S1 · Alarm paketi · deterministik | 0.10 | 91 | 15 | 7 | 8 | 19 | 1 |
| S2 · Alarm paketi · Qwen açık | 5.41 | 91 | 15 | 7 | 8 | 19 | 1 |
| S3 · Ham log · soğuk registry | 2.89 | 44 | 40 | 9 | 31 | 3 | 7 |
| S4 · Ham log · sıcak registry | **0.16** | 44 | 40 | 9 | 31 | 3 | 7 |

**Türetilmiş metrikler**

| Metrik | Değer |
|---|---|
| Gürültü azaltma (alarm paketi) | **%53.3** (15 adaydan 8'i bastırıldı) |
| Alarm → olay yoğunlaşması | 91 alarm → **1 olay** (%98.9 azalma) |
| Ham log segmentasyonu | 246 fiziksel satır → **44 mantıksal olay** |
| Registry hızlanması (soğuk → sıcak) | 2.89s → 0.16s = **~18x**, AI çağrısı **1 → 0** |
| Qwen case analizi | 1 çağrı · 5.4–5.7s · ~7.1k token |
| Deterministik çekirdek çalışma süresi | **0.10–0.17 s** |

**Doğruluk kontrolleri — 7/7 GEÇTİ**

| Kontrol | Sonuç |
|---|---|
| K1 arka plan gürültüsü bastırıldı (`cpu_high`/`backup_warn`/`ntp_drift`) | GEÇTİ |
| K2 kök varlık doğru (`billing-db`) | GEÇTİ |
| K2 kök alarm tipi doğru (`disk_full`) | GEÇTİ |
| K3 tek nedensel zincir tek olay | GEÇTİ |
| K4 tekrarlanabilirlik — deterministik sayılar koşular arası birebir aynı | GEÇTİ |
| K5 AI olmadan da olay üretildi | GEÇTİ |
| K6 her nitelikli sinyalde kanıt var | GEÇTİ |

### Tekrarlanabilirlik gözlemi (önemli)

İki ardışık koşuda **deterministik sayıların hepsi birebir aynı** çıktı
(15/7/8/19/1 ve 40/9/31/3/7). Değişen tek şey AI'a bağlı kısımlardı:

| Koşu | DeepSeek sonucu | S3 süresi |
|---|---|---|
| 1 | Yanıt `finish_reason=length` ile kesildi → **deterministik fallback** devreye girdi, coverage %100 | 6.36 s |
| 2 | AI regex kabul edildi, coverage %100 | 2.89 s |

Aynı girdi, LLM'den iki farklı davranış — ve **çıktı ikisinde de aynı**.
Mimarinin varlık nedeni tam olarak budur: LLM değişken, ürün değil.

---

## Yapılamayanlar

- Commit geçmişi seyrek; geliştirme süreci commit'lerle değil kod izleriyle
  belgeleniyor.
- Resmî hackathon veri paketiyle ölçüm yapılmadı; ölçümler
  `tests/fixtures/sre_segmentation_test_same_format_changed_content.log` ve
  `tools/benchmark.py`'nin ürettiği sentetik pakete dayanıyor.
- Otomatik test paketi (pytest) yok; doğrulama `tools/benchmark.py` içindeki
  7 kontrolle yapılıyor.
- Deploy edilmiş bir URL yok; uygulama yerel Streamlit olarak çalışır.
