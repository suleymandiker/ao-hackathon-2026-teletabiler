# Plan — AOP (Ajan Tabanlı Operasyon Platformu)

## 1. Problem tanımı

Bir SRE / NOC ekibi vardiya başına on binlerce alarm ve log satırı görür.
Gerçek sorun "az veri" değil, **gürültü içinde nedenselliği kaybetmek**tir:

| Semptom | Operasyonel sonuç |
|---|---|
| Aynı arızanın 200 farklı alarmı | Alarm yorgunluğu; kritik alarm gözden kaçar |
| `timeout`, `http_5xx`, `latency_high` aynı anda patlar | Hangisi sebep, hangisi sonuç bilinmez |
| Arka plan telemetrisi (`cpu_high`, `backup_warn`) kritik alarmla karışır | Yanlış önceliklendirme |
| Her ekip kendi servisinin alarmını görür | Zincirin tamamı kimsede yok |
| Log formatı ekipten ekibe değişir | Ortak araç yazılamaz |

Bu hackathon'un verdiği paket tam olarak bu tabloyu modelliyor: `alarms` +
`host_inventory` + `service_dependencies`. Yani ham alarm akışının yanında
**topoloji bağlamı** da var — ve nedenselliği kurmanın anahtarı orada.

### Neden mevcut çözümler yetmiyor

- **Kural motorları** (Nagios/Zabbix korelasyon kuralları): her yeni servis
  için elle kural yazmak gerekir, ölçeklenmez.
- **"Logları LLM'e ver, o söylesin"**: 100 GB log bağlam penceresine sığmaz;
  maliyet kontrolsüzdür; en kötüsü **her çalıştırmada farklı cevap** verir.
  Bir olay kaydına imza atacak operatör için bu kabul edilemez.

---

## 2. Hedef

> **Gürültüyü deterministik olarak eleyip, topoloji-farkında bir nedensellik
> zinciri kurmak ve her adımın kanıtını operatöre göstermek. LLM'i yalnız
> insanın gerçekten yavaş olduğu iki yerde kullanmak: bilinmeyen bir log
> formatını çözmek ve hazır kanıtı uzman diline çevirmek.**

### Başarı kriterleri (proje başında konulan)

| # | Kriter | Ölçüm |
|---|---|---|
| K1 | Gürültü belirgin biçimde azalmalı | Bastırılan sinyal adayı oranı |
| K2 | Kök neden **doğru varlığa** işaret etmeli | `probable_root.entity` beklenen kök servise eşit mi |
| K3 | Bağımsız arızalar birleşmemeli | Bağımsız kök sayısı = olay sayısı |
| K4 | Aynı girdi → aynı çıktı | Ardışık koşularda sayıların birebir aynı olması |
| K5 | LLM erişilemese de sistem çalışmalı | `use_ai_rca=False` ile tam pipeline |
| K6 | Her karar kanıtlanabilir olmalı | Her katman çıktısında `evidence` alanı |
| K7 | Bilinen formatta AI maliyeti sıfır olmalı | İkinci koşuda AI çağrı sayısı |

Ölçülen sonuçlar: [docs/fazlar.md](fazlar.md) ve [AI_JURI.md](../AI_JURI.md).
Tekrar üretmek için: `python tools/benchmark.py`.

---

## 3. Yaklaşım — üç temel karar

### Karar 1: Deterministik çekirdek, AI kenarda

Alternatifler değerlendirildi:

| Seçenek | Artı | Eksi | Karar |
|---|---|---|---|
| Uçtan uca LLM ajanı | Hızlı prototip, "havalı" demo | Tekrarlanamaz, pahalı, bağlam sınırı, halüsinasyon riski | ✗ |
| Saf kural motoru | Deterministik, ucuz | Bilinmeyen format = elle regex yazmak | ✗ |
| **Deterministik veri düzlemi + AI kontrol düzlemi** | Tekrarlanabilir, ucuz, yine de yeni formata uyum sağlar | Daha fazla mühendislik işi | ✓ |

**Neden:** Operatör bir olay kaydına imza atar. İmza atılan bir çıktı
tekrarlanabilir olmak zorundadır.

### Karar 2: Nedensellik topolojiden gelir, zamandan değil

Zaman yakınlığı en yaygın korelasyon sinyalidir ve en yanıltıcı olanıdır.
Bir DB arızası ile ilgisiz bir sertifika uyarısı aynı saniyede olabilir.

AOP'de bir topolojik korelasyon kenarının kurulması için **üç şart birden**
gerekir: yönlü bağımlılık + nedensel uyum + kök-önce zaman sırası.
Bkz. [mimari.md § Katman 6](mimari.md).

### Karar 3: AI çağrısı öğrenilir, tekrarlanmaz

Bir log formatı bir kez çözülür, doğrulanır ve SQLite registry'ye yazılır.
İkinci kez aynı formatla karşılaşıldığında AI çağrısı **sıfırdır**. Bu, AI'ı
"her istekte para yakan bir bağımlılık" olmaktan çıkarıp "bir kerelik
öğrenme maliyeti" haline getirir.

---

## 4. Kapsam

### Kapsam içi
- ZIP paket girişi (alarms + host_inventory + service_dependencies)
- Tekil dosya girişi: CSV / JSON / TXT / MD / LOG
- 9 katmanlı deterministik pipeline
- Çok satırlı ham log segmentasyonu (traceback, stack trace, gömülü JSON)
- Bilinmeyen format için AI destekli, doğrulamalı politika keşfi
- Topoloji-farkında korelasyon ve root-seeded olay üretimi
- Deterministik RCA + Qwen uzman case yorumu
- İnsan onaylı aksiyon önerisi (otomatik yürütme yok)
- Katman katman XAI izleme arayüzü

### Kapsam dışı (bilinçli)
- Otomatik remediation / self-healing — güven modeli buna izin vermiyor
- Gerçek zamanlı akış (Kafka/stream) — girdi dosya/paket tabanlı
- Kalıcı olay veritabanı ve vardiya devri — tek oturumluk analiz
- Çoklu kullanıcı / yetkilendirme
- Metrik ve trace kaynakları (yalnız log + alarm)

---

## 5. İnsan / AI iş bölümü

| Karar | Kim verdi | Gerekçe |
|---|---|---|
| Deterministik çekirdek ilkesi | **İnsan** | Ürün güven modelinin temeli; AI'a devredilemez |
| 9 katmanın sınırları ve sözleşmeleri | **İnsan** | Mimari sahiplik |
| `ROOT / SYMPTOM / BACKGROUND` alarm sınıfları | **İnsan** | SRE alan bilgisi |
| Korelasyon skor ağırlıkları ve eşikler | **İnsan** | Ürün davranışını belirler, kalibrasyon işi |
| Root-seeded kümeleme kararı | **İnsan** | Bağlı-bileşen yaklaşımının hatası gözlemlenerek alındı |
| Katman gövdelerinin kodlanması | **AI (Claude Opus)** | Yoğun, tekrarlı, sözleşmesi net iş |
| Streaming örnekleyici, regex doğrulayıcı | **AI (Claude Opus)** | Kenar durumu bol, test edilebilir |
| Streamlit tasarım sistemi (`theme.py`) | **AI (Claude Opus)** | Token'lar insan tarafından verildi, üretim AI |
| Bilinmeyen log formatı için regex | **AI (DeepSeek, çalışma zamanı)** | İnsanın dakikalar süren işi; **deterministik doğrulamadan geçmek zorunda** |
| Olay/case uzman yorumu | **AI (Qwen, çalışma zamanı)** | Kanıt zaten hazır; yalnız operatör diline çevirir |
| Neyin olay olduğu | **Kod (deterministik)** | Hiçbir AI'a devredilmedi |

Detay ve prompt kanıtları: [prompts/](../prompts/) ve [CLAUDE.md](../CLAUDE.md).

---

## 6. Risk kaydı

| Risk | Etki | Azaltma | Durum |
|---|---|---|---|
| LLM gateway erişilemez | Analiz durur | Her AI yolunda deterministik fallback | Kapandı |
| LLM bozuk/alakasız JSON döndürür | Yanlış RCA gösterimi | Şema + semantik kapı ([rca_engine.py:201](../src/backend/rca_layer/rca_engine.py#L201)) | Kapandı |
| AI regex logu yanlış böler | Tüm downstream bozulur | Tam akışta `RegexValidator`; geçemezse fallback | Kapandı |
| Prompt injection (log içinde talimat) | Model kandırılır | [common_system.md](../src/backend/prompts/common_system.md) sözleşmesi + payload "untrusted data" | Kapandı |
| İki bağımsız arıza tek olaya çöker | Yanlış kök neden | Root-seeded kümeleme | Kapandı |
| Büyük dosya RAM'i doldurur | Çökme | Streaming generator + bayt aralıklı örnekleme | Kapandı |
| Log/LLM metni arayüzde HTML enjeksiyonu | XSS | `esc()` + bileşen düzeyinde kaçışlama | Kapandı |
| API anahtarı sızıntısı | Güvenlik | `.env` gitignore'da, `.env.example` şablon | Kapandı |
| Registry bozulursa keşif tekrarlanır | Maliyet | Kabul edilen risk; registry silinebilir/yeniden üretilebilir | Kabul |

---

## 7. Yol haritası (bu hackathon sonrası)

1. Operatör geri bildirimi → gürültü kapısı eşiklerinin otomatik kalibrasyonu
2. Zaman serisi metrik kaynağı (Prometheus) ile alarm kanıtının desteklenmesi
3. Olay geçmişi kalıcılığı ve "benzer geçmiş olay" önerisi
4. Streaming girdi (Kafka) ve sürekli çalışan pencere
5. Onaylı runbook tetikleme (yalnız operatör onayıyla)
