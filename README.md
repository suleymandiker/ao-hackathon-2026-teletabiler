# AOP — Ajan Tabanlı Operasyon Platformu

> Ham log ve alarm akışını, her adımı kanıtla açıklanabilir tek bir kök neden
> hipotezine dönüştüren deterministik SRE zekâ hattı — AI'ı yalnız bilinmeyen
> log formatını çözmek ve hazır kanıtı uzman diline çevirmek için kullanır.

**Takım:** teletabiler · **Hackathon:** AO Hackathon 2026 · **Platform:** SAKA

---

## 1. Çözdüğümüz problem

Bir SRE / NOC ekibi vardiya başına on binlerce alarm görür. Gerçek sorun az
veri değil, **gürültü içinde nedenselliği kaybetmek**tir:

| Semptom | Operasyonel sonuç |
|---|---|
| Aynı arızanın yüzlerce alarmı | Alarm yorgunluğu; kritik alarm gözden kaçar |
| `timeout`, `http_5xx`, `latency_high` aynı anda patlar | Hangisi sebep, hangisi sonuç bilinmez |
| Arka plan telemetrisi (`cpu_high`, `backup_warn`) kritik alarmla karışır | Yanlış önceliklendirme |
| Her ekip kendi servisinin alarmını görür | Zincirin tamamı kimsede yok |
| Log formatı ekipten ekibe değişir | Ortak araç yazılamaz |

Kolay ama yanlış çözüm: *"Logları LLM'e ver, o söylesin."* 100 GB log bağlam
penceresine sığmaz, maliyet kontrolsüzdür ve en kötüsü **her çalıştırmada
farklı cevap** verir. Bir olay kaydına imza atacak operatör için bu kabul
edilemez.

---

## 2. Çözümümüz nasıl çalışıyor

### Temel ilke: deterministik çekirdek, AI kenarda

```
┌──────────── KONTROL DÜZLEMİ (seyrek, AI'lı) ─────────────┐
│  Bilinmeyen format? → DeepSeek keşif → DOĞRULA → sakla   │
│  Olay üretildi?     → Qwen uzman yorumu (karar vermez)   │
└───────────────────────────┬──────────────────────────────┘
                            │ doğrulanmış politika
                            ▼
┌──────────── VERİ DÜZLEMİ (%100 deterministik) ───────────┐
│  streaming → parse → şablon → pencere → gürültü kapısı → │
│  korelasyon → olay → RCA hipotezi → plan                 │
│  Bu yolda tek bir LLM çağrısı yoktur.                    │
└──────────────────────────────────────────────────────────┘
```

**Bir LLM çıktısı hiçbir zaman bir olayın varlığına, bir korelasyonun
kurulmasına veya bir sinyalin nitelikli sayılmasına karar veremez.**

### 9 katmanlı hat

| # | Katman | Ne yapar | Kanıt alanı |
|---|---|---|---|
| 0 | **Input Package** | ZIP'teki 3 dosyayı kolon imzasıyla tanır, alarm ↔ envanter çapraz doğrulaması yapar | `data_quality` |
| 1 | **Segmentation** | Çok satırlı kaydı (traceback, stack trace) tek mantıksal olaya birleştirir. Bilinmeyen format için DeepSeek keşfi + tam akış doğrulaması | `validation.coverage` |
| 2 | **Parser** | 7 parser ailesi → tek tip `CanonicalEvent`. Downstream log formatını bilmez | `template_source` |
| 3 | **Template** | Drain3 + maskeleme + yapısal genelleme → `template_id` | `template_reliable` |
| 4 | **Aggregation** | 60 sn pencerede `(alarm_type, template, servis)` → sinyal adayı | `burst_score` |
| 5 | **Qualification** | Açıklanabilir gürültü kapısı (eşik 0.60) | `qualification_evidence` |
| 6 | **Correlation** | Topoloji yönü + nedensel uyum + kök-önce zaman sırası | `evidence`, `dependency_path` |
| 7 | **Incident** | Root-seeded kümeleme — bağımsız arızalar birleşemez | `probable_root` |
| 8 | **RCA** | Deterministik hipotez sıralaması + tek Qwen uzman case yorumu | `root_cause_candidates[].evidence` |
| 9 | **Planning** | Sahip ataması + güvenli sonraki adım. `execution_allowed=False` | `requires_human_approval` |

Ayrıntılı mimari: **[docs/mimari.md](docs/mimari.md)**

### Ölçülen sonuçlar

`python tools/benchmark.py` · Windows 11 · Python 3.10

| Metrik | Değer |
|---|---|
| Gürültü azaltma (alarm paketi) | **%53.3** — 15 sinyal adayının 8'i bastırıldı |
| Alarm → olay yoğunlaşması | **91 alarm → 1 olay** (%98.9 azalma) |
| Kök neden doğruluğu | `billing-db` / `disk_full` — beklenenle birebir |
| Ham log segmentasyonu | 246 fiziksel satır → **44 mantıksal olay**, kayıp 0 |
| Registry hızlanması (soğuk → sıcak) | **2.89 s → 0.16 s (~18x)**, AI çağrısı **1 → 0** |
| Deterministik çekirdek süresi | **0.10 – 0.17 s** |
| Qwen case analizi | 1 çağrı · 5.4 s · ~7.1k token · güven 0.95 |
| Doğruluk kontrolleri | **7/7 GEÇTİ** |

Tekrarlanabilirlik: iki ardışık koşuda **deterministik sayıların tamamı
birebir aynı** çıktı. Bir koşuda DeepSeek yanıtı token limitinde kesildi;
sistem deterministik fallback'e düştü ve **aynı 44 mantıksal olayı** üretti.

Faz faz süreç ve tüm ölçümler: **[docs/fazlar.md](docs/fazlar.md)**

---

## 3. Kurulum

### Ön koşullar
- Python **3.10+** (3.10 ile test edildi)
- Turkcell/SAKA inference gateway'e ağ erişimi (opsiyonel — yoksa sistem
  deterministik modda çalışır)

### Adımlar

```bash
# 1. Repoyu alın
git clone <repo-url>
cd ao-hackathon-2026-teletabiler

# 2. Sanal ortam (önerilir)
python -m venv .venv
source .venv/bin/activate        # Windows PowerShell: .\.venv\Scripts\Activate.ps1

# 3. Bağımlılıklar
pip install -r requirements.txt

# 4. Ortam değişkenleri
cp .env.example .env             # Windows PowerShell: Copy-Item .env.example .env
# .env içindeki SAKA_API_KEY alanını doldurun.
```

`.env` **asla commit edilmez** — `.gitignore` içinde tanımlıdır.
Tüm değişkenlerin açıklaması: [.env.example](.env.example)

---

## 4. Çalıştırma

### Arayüz (birincil)

```bash
streamlit run src/frontend/streamlit_app.py
```

> Komutu **proje kökünden** çalıştırın — tema `.streamlit/config.toml`
> dosyasından okunur. Uygulama `http://localhost:8501` adresinde açılır.

Sol panelden veri paketini yükleyip **"◈ Analizi Başlat"** düğmesine basın.

**Desteklenen girdiler**

| Format | İçerik |
|---|---|
| `.zip` *(önerilen)* | `alarms.json` + `host_inventory.csv` + `service_dependencies.csv` — üç dosya isimle değil **kolon imzasıyla** tanınır |
| `.json` / `.csv` | Tekil alarm dosyası (topoloji bağlamı olmadan) |
| `.log` / `.txt` / `.md` | Ham log akışı — tam segmentasyon + parser zinciri çalışır |

**İki görünüm**
- **Operasyon Analizi** — KPI'lar, sinyal tablosu, olay listesi, olay başına
  5 sekmeli XAI incelemesi
- **Katman İzleme** — 9 katmanın her biri için *Girdi → Uygulanan Karar →
  Çıktı* ve o katmanın **gerçek** pipeline çıktısı

### Ölçüm koşusu (jüri için en hızlı doğrulama)

```bash
python tools/benchmark.py            # tüm senaryolar (AI dahil)
python tools/benchmark.py --offline  # hiç AI çağrısı yapma
```

Sentetik bir alarm paketi üretir, 4 senaryo koşar, sonuç tablosunu ve 7
doğruluk kontrolünü yazdırır. Tüm kontroller geçerse çıkış kodu `0`.

### Kütüphane olarak

```python
import sys; sys.path.insert(0, 'src/backend')
from full_pipeline_v2 import FullAIOpsPipelineV2

pipeline = FullAIOpsPipelineV2(use_ai_rca=True)
result = pipeline.process_package('alarm_paketi.zip')   # veya .process_file('app.log')

print(result['stats'])
print(result['incidents'][0]['probable_root'])
print(result['case_analysis']['kok_neden_hipotezi'])
```

---

## 5. Kullanılan AI araçları ve model sürümleri

### Çalışma zamanı modelleri (ürünün içinde)

| Rol | Model | Sürüm / ID | Kullanım | Ne zaman çağrılır |
|---|---|---|---|---|
| `Segmentation_Discovery` | DeepSeek | `deepseek-v4-flash-0731` | analiz | Yalnız **bilinmeyen** log format imzası ilk kez görüldüğünde |
| `Parser_Discovery` | DeepSeek | `deepseek-v4-flash-0731` | analiz | Yalnız yerleşik parser ailelerinin hiçbiri uymadığında |
| `Ajan_2_RCA_Expert` | Qwen | `ai-genai__qwen35-122b-a10b-awq-ai-genai` | açıklama | Olay üretildikten **sonra**, tüm case için **tek** çağrı |

Erişim: Turkcell ortak inference gateway (OpenAI-uyumlu API), tek
`SAKA_API_KEY` ile. Tek erişim noktası:
[src/backend/ai_engine.py](src/backend/ai_engine.py)

### Geliştirme araçları (kodu yazarken)

| Araç | Model | Kullanım |
|---|---|---|
| Claude Code (VS Code eklentisi) | **Claude Opus 5** | Katman gövdelerinin kodlanması, regex doğrulayıcı, Streamlit tasarım sistemi, belgeleme |
| Kalıcı yönerge dosyası | — | [CLAUDE.md](CLAUDE.md) — her oturumda yüklenir, mimari kısıtları zorunlu kılar |

Kullanılan prompt'lar ve savunma katmanları: **[prompts/](prompts/)**

### İnsan / AI iş bölümü

| Karar | Kim |
|---|---|
| Deterministik çekirdek ilkesi, 9 katman sözleşmesi | **İnsan** |
| Alarm semantik sınıfları (`ROOT` / `SYMPTOM` / `BACKGROUND`) | **İnsan** |
| Korelasyon skor ağırlıkları ve eşikler | **İnsan** |
| Root-seeded kümeleme kararı | **İnsan** |
| Katman gövdelerinin kodlanması, arayüz üretimi | **AI (Claude Opus 5)** |
| Bilinmeyen log formatı için regex | **AI (DeepSeek, çalışma zamanı)** — deterministik doğrulamadan geçmek zorunda |
| Olay/case uzman yorumu | **AI (Qwen, çalışma zamanı)** — karar üretmez |
| **Neyin olay olduğu** | **Kod (deterministik)** — hiçbir AI'a devredilmedi |

---

## 6. MCP sunucu listesi

**Kullanılmadı.** Bu projede hiçbir MCP (Model Context Protocol) sunucusu
yapılandırılmamıştır.

Gerekçe: Ürünün tek dış bağımlılığı Turkcell inference gateway'dir ve buna
doğrudan HTTP (OpenAI-uyumlu) ile erişilir
([ai_engine.py:92-186](src/backend/ai_engine.py#L92-L186)). Araya bir MCP
katmanı koymak, "tek LLM erişim noktası" ilkesini zayıflatır ve jüri
ortamında ek kurulum gerektirirdi.

---

## 7. Entegre edilen API'ler

| API | Uç nokta | Protokol | Kullanım |
|---|---|---|---|
| Turkcell SAKA — DeepSeek | `common-inference-apis.turkcelltech.ai/llm-dynamo-deepseek-v4-flash-0731/v1/chat/completions` | OpenAI-uyumlu `chat/completions` | Segmentasyon + parser politikası keşfi |
| Turkcell SAKA — Qwen | `common-inference-apis.turkcelltech.ai/qwen35-122b-a10b-awq-ai-genai/v1/chat/completions` | OpenAI-uyumlu `chat/completions` | Olay/case uzman yorumu |

Kimlik doğrulama: `Authorization: Bearer ${SAKA_API_KEY}`.
Zaman aşımı: bağlantı 15 s / okuma 180 s. Gateway `response_format` kabul
etmezse istek bir kez sade olarak tekrarlanır.

**Başka hiçbir dış API kullanılmamaktadır.** Veritabanı olarak yalnız yerel
SQLite (`data/policy_registry.sqlite3`) kullanılır.

---

## 8. Ekran görüntüleri

Görseller ve nasıl çekilecekleri: **[demo/](demo/)**

Arayüzün gösterdiği ana bölümler:

| Bölüm | İçerik |
|---|---|
| Veri Paketi | Alarm / host / bağımlılık sayıları + alarm ↔ envanter tutarlılık kontrolü |
| Operasyon Özeti | 7 KPI: log olayı, benzersiz şablon, sinyal adayı, nitelikli sinyal, bastırılan gürültü, korelasyon bağı, olay adayı |
| İşlem Hattı | 9 katmanın akış şeridi, her katmanın kendi birimiyle |
| Sinyal Analizi | En yüksek puanlı 100 sinyal — bastırılanlar dahil, kanıt etiketleriyle |
| Olay ve XAI İncelemesi | Olası ana sorun + güven ölçeri + 5 sekmeli kanıt incelemesi |
| Katman İzleme | 9 katman için *Girdi → Uygulanan Karar → Çıktı* + gerçek pipeline çıktısı |

Tasarım sistemi ve tema ayrıntıları: [STREAMLIT_UI.md](STREAMLIT_UI.md)

---

## 9. Deploy URL ve bilinen sınırlar

### Deploy URL

**Yok.** Uygulama yalnız yerel Streamlit olarak çalışır
(`http://localhost:8501`).

Gerekçe: Turkcell inference gateway kurumsal ağ içindedir; public bir deploy
`SAKA_API_KEY`'i dışarıya taşımayı gerektirirdi. Jüri değerlendirmesi için
**[§4 Çalıştırma](#4-çalıştırma)** adımları veya tek komutluk
`python tools/benchmark.py` koşusu yeterlidir.

### Bilinen sınırlar

**Doğrulama ve veri**
- Resmî hackathon veri paketiyle ölçüm yapılmadı. Sayılar
  `data/sre_segmentation_test_same_format_changed_content.log` dosyasına ve
  `tools/benchmark.py`'nin ürettiği **sentetik** alarm paketine dayanır.
- Otomatik test paketi (pytest) yok. Doğrulama `tools/benchmark.py` içindeki
  7 kontrolle yapılır.
- Commit geçmişi seyrek (4 commit); geliştirme süreci
  [docs/fazlar.md](docs/fazlar.md) ile belgelenmiştir.

**Ürün kapsamı**
- Otomatik remediation / self-healing **yok** ve bilinçli olarak
  eklenmeyecek. Sistem yalnız öneri üretir; `execution_allowed=False`.
- Gerçek zamanlı akış (Kafka vb.) yok — girdi dosya/paket tabanlıdır.
- Olay geçmişi kalıcı değildir; analiz tek oturumluktur.
- Yalnız log + alarm kaynakları; metrik ve distributed trace entegre değil.
- "Learning" katmanı şu an yalnız planlama üretir; operatör geri bildirimini
  registry'ye geri besleyen döngü kurulmamıştır.

**Teknik**
- `TopologyContext` statiktir; paket içindeki bağımlılık dosyası neyse odur.
- Korelasyon penceresi O(n²) tarar; zaman sırası + erken `break` ile pratikte
  doğrusala yakın, ancak çok geniş pencerelerde maliyet artar.
- Şablon durumu diske yazılır; çok işlemli eşzamanlı yazım için kilit yok.
- Çok kullanıcılı erişim / yetkilendirme yok.

---

## 10. Repo haritası

```
├── README.md                  bu dosya
├── AI_JURI.md                 AI Jüri için yapılandırılmış özet
├── submission.json            makine okunabilir künye
├── CLAUDE.md                  AI kodlama asistanı yönergesi
├── .env.example               ortam değişkeni şablonu
├── docs/
│   ├── plan.md                problem, hedef, kapsam, risk kaydı
│   ├── mimari.md              katman katman tasarım kararları
│   └── fazlar.md              geliştirme fazları + ölçüm tablosu
├── prompts/
│   ├── README.md              prompt envanteri ve savunma katmanları
│   └── gelistirme/            AI asistanı yönlendirme kalıpları
├── demo/                      ekran görüntüleri
├── tools/benchmark.py         tekrarlanabilir ölçüm koşusu
├── data/                      smoke test log dosyası + politika registry'si
└── src/
    ├── backend/               9 katmanlı pipeline + ai_engine + prompts
    └── frontend/              Streamlit arayüzü + tasarım sistemi
```
