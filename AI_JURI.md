# 🏆 teletabiler-ai-ops

> **Loglardan sinyale, sinyalden olaya, olaydan kök nedene ve aksiyona
> uzanan açıklanabilir AIOps platformu.**

## 1. AI Stratejimiz ve İş Akışı

**teletabiler-ai-ops**, yüksek hacimli ve gürültülü log/alarm akışlarını
doğrudan bir LLM'e gönderip sonuç bekleyen bir sistem değildir.
Platform; logları önce deterministik ve izlenebilir bir işlem hattından
geçirir, anlamlı sinyalleri ayırır, zaman ve servis bağımlılıklarını
kullanarak ilişkili sinyalleri olaylara dönüştürür ve AI'ı doğru noktada
uzman analiz katmanı olarak kullanır.

``` text
Raw Logs / Alarm Stream
        ↓
Segmentation
        ↓
Parser & CanonicalEventV2
        ↓
Template Layer V4-F
        ↓
Signal Candidate / Aggregation
        ↓
Deterministic Qualification & Noise Suppression
        ↓
Qualified Signals
        ↓
Deterministic Correlation
(Time + Topology + Dependency Direction + Causal Order)
        ↓
Incident Candidate
        ↓
Context Enrichment
        ↓
Deterministic RCA
        ↓
Qwen Expert Analysis
        ↓
Action / Plan
        ↓
Operations UI
```

### AI'ı nerede ve neden kullanıyoruz?

AI, sistemin tamamının yerine geçen bir karar mekanizması değildir.
Kritik incident discovery ve korelasyon adımlarını mümkün olduğunca
**deterministik, tekrar üretilebilir ve açıklanabilir** tutuyoruz.

-   **Discovery ajanları:** Yeni veya bilinmeyen log formatlarında
    segmentation/parser politikalarının keşfedilmesine yardımcı olur.
-   **Deterministik çekirdek:** Normalizasyon, template çıkarımı, signal
    qualification, noise suppression, correlation ve incident discovery
    işlemlerini kontrol edilebilir kurallarla yürütür.
-   **Qwen Expert:** Oluşturulmuş incident ve kanıt paketini vaka
    seviyesinde yorumlar; RCA açıklaması ve aksiyon önerisinin
    zenginleştirilmesinde kullanılır.
-   **Human-in-the-loop:** Operasyonel aksiyon, owner ve durum bilgileri
    kullanıcı tarafından izlenebilir; AI çıktısı kanıt zincirinden
    bağımsız bir "kara kutu karar" değildir.

Bu tasarım sayesinde LLM hallucination riskini incident discovery
aşamasından uzaklaştırırken GenAI'ın yorumlama ve uzmanlık avantajından
yararlanıyoruz.

### Son Git Commitleri

``` text
20da240 feat: AI konusma gecmisi icin conv.history.md otomatik loglama eklendi
477d4ea Initial commit
```

-   **Kanıt Dosyaları:** `prompts/`, `docs/plan.md`, `CLAUDE.md`,
    `conv.history.md`

------------------------------------------------------------------------

## 2. Problemi Nasıl Çözdük

Operasyon ekiplerinin temel problemi "log yokluğu" değil, **çok fazla
alarm içinden gerçekten aynı probleme ait olanları bulmak ve ilk
bakılması gereken noktayı belirlemektir**.

### 2.1 Heterojen logları ortak bir dile dönüştürme

JSON, structured text, syslog, Kubernetes, Nginx, Java stack trace ve
benzeri log formatları segmentation ve parser katmanlarından geçirilir.
Parser çıktısı ortak `CanonicalEventV2` modeline dönüştürülür.

``` text
Farklı Log Formatları → CanonicalEventV2 → Tek Tip Downstream Processing
```

### 2.2 Alarm gürültüsünü azaltma

Her alarm incident değildir. Template, aggregation ve qualification
katmanları tekrar eden veya operasyonel değeri düşük olayları ayırarak
correlation katmanına daha anlamlı bir sinyal seti gönderir.

``` text
Binlerce Alarm → Template / Aggregation → Qualification → Noise Suppression → Qualified Signals
```

### 2.3 Aynı zamanda olanı değil, aynı probleme ait olanı bulma

Correlation yalnızca zaman yakınlığına dayanmaz:

``` text
TIME + TOPOLOGY + DEPENDENCY DIRECTION + ALARM SEMANTICS + CAUSAL ORDER
                              ↓
                       INCIDENT EVIDENCE
```

Servis bağımlılığı `source → target` ise arıza yayılımı ters yönde
incelenir:

``` text
dependency: A → B → C
failure:    C → B → A
```

### 2.4 "İlk ne tetikledi?" sorusunu cevaplama

Sistem yalnızca alarm kümeleri oluşturmayı değil, olay zincirindeki
**earliest credible trigger** noktasını belirlemeyi hedefler.

``` text
payment-provider-gw / ext_unreach
             ↓
payment-service / timeout
             ↓
order-service / txn_fail
             ↓
mobile-bff / txn_fail
```

Root seçimi yalnızca alarm tipine göre yapılmaz. Zaman sırası ile
dependency graph üzerindeki failure propagation yönünün birbirini
desteklemesi beklenir.

Her incident candidate için kök neden hipotezi, root alarm, etkilenen
servisler, alarm sayısı, zaman aralığı, güven bilgisi, RCA, ilk aksiyon,
owner ve durum üretilebilir.

-   **Kanıt Dosyaları:** `src/backend/`,
    `src/frontend/streamlit_app.py`, `docs/mimari.md`, `demo/`

------------------------------------------------------------------------

## 3. X-Factor Özelliklerimiz

### 🚀 X-Factor 1: AI Destekli Log Format Keşfi + Deterministik Normalizasyon

Yeni/bilinmeyen formatlarda AI discovery mekanizması uygun
segmentation/parser politikasının keşfedilmesine yardımcı olur;
keşfedilen politika sonrasında deterministik olarak uygulanabilir.

> **AI keşfeder; üretim akışı deterministik olarak uygular.**

-   Multi-format segmentation

-   Timestamp/severity/message normalizasyonu

-   Structured ve unstructured log desteği

-   CanonicalEventV2

-   Policy registry

-   Parser/segmentation QA testleri

-   **Kanıt:** `src/backend/segmentation_layer/`,
    `src/backend/parser_layer/`, ilgili `tools/*qa*.py`

### 🚀 X-Factor 2: Template Mining ile Gürültüden Yapısal Sinyale Geçiş

Runtime ID, request ID veya değişken alanlar yüzünden aynı operasyonel
olayın binlerce farklı mesajmış gibi değerlendirilmesini önlemek için
yapısal log pattern'leri çıkarılır.

``` text
Raw Message → Template / Fingerprint → Stable Event Pattern → Aggregation
```

-   **Kanıt:** `src/backend/template_layer/`, template QA/regression
    testleri

### 🚀 X-Factor 3: Deterministik Noise Gate

Her alarmı AI'a göndermek yerine deterministic qualification katmanı
operasyonel değeri düşük sinyalleri correlation öncesinde bastırır.

``` text
Signal Candidates
       ↓
Qualification
   ↙          ↘
Noise       Qualified
              ↓
          Correlation
```

Bu sayede LLM context'i küçülür, korelasyon uzayı daralır ve karar
davranışı daha öngörülebilir olur.

-   **Kanıt:** `src/backend/aggregation_layer/`,
    `src/backend/signal_qualification_layer/`

### 🚀 X-Factor 4: Time + Topology + Dependency Direction ile Causal Correlation

Projenin en ayırt edici teknik özelliklerinden biridir. Sistem yalnızca
"aynı zaman penceresinde oluştu" demek yerine zaman, servis topolojisi,
dependency yönü ve alarm semantiğini birlikte değerlendirir.

``` text
Root Trigger → Dependent Service → Downstream Symptom → Incident Evidence
```

-   **Kanıt:** `src/backend/correlation_layer/`,
    `src/backend/incident_candidate_layer/`, `service_dependencies.csv`,
    `host_inventory.csv`

### 🚀 X-Factor 5: Earliest-Trigger Causal Validation

Sistemin incident sonucunu körü körüne doğru kabul etmiyoruz. Bağımsız
validator şu soruyu sorar:

> **"Bu olayın ilk güvenilir tetikleyicisi gerçekten bu mu ve sonraki
> alarmlar bu kökten zaman/topoloji yönünde açıklanabiliyor mu?"**

Validator alarmları timestamp sırasına dizer, dependency graph'taki
failure propagation yönünü takip eder ve root sonrası semptomları
kanıtlamaya çalışır.

-   **Kanıt:** `tools/first_trigger_causal_validator.py`,
    downstream/regression QA araçları

### 🚀 X-Factor 6: Explainable AIOps --- Raw Log'dan RCA'ya Karar İzi

``` text
Raw Log
   ↓
Canonical Event
   ↓
Template
   ↓
Signal Candidate
   ↓
Qualified Signal
   ↓
Correlation
   ↓
Incident
   ↓
RCA
   ↓
Action
```

Amaç yalnızca sonuç göstermek değil; alarmın neden noise olduğu, neden
incident'e girdiği, hangi dependency'nin kullanıldığı ve RCA'nın hangi
kanıta dayandığını açıklayabilmektir.

-   **Kanıt:** `src/backend/full_pipeline_v2.py`,
    `src/backend/downstream_pipeline.py`,
    `src/frontend/streamlit_app.py`

### 🚀 X-Factor 7: Deterministik Incident Discovery + Qwen Expert

LLM'i ham alarm akışının üzerine koymak yerine uzman katmanı olarak
konumlandırdık.

``` text
Deterministic Evidence
        ↓
Incident Candidate
        ↓
Deterministic RCA
        ↓
Qwen Expert
        ↓
Explanation + Recommended Action
```

Qwen, oluşturulmuş vaka paketini yorumlayarak kök neden açıklamasını ve
önerilen ilk aksiyonu zenginleştirir. Incident discovery'nin temel
sonucu LLM cevabına bağımlı değildir.

-   **Kanıt:** `src/backend/ai_engine.py`,
    `src/backend/prompt_registry.py`, `src/backend/prompts/`,
    `src/backend/rca_layer/`, `src/backend/learning_planning_layer/`

------------------------------------------------------------------------

## 4. Jüriye Göstermek İstediğimiz Teknik Fark

> **Biz alarm özetleyen bir GenAI uygulaması geliştirmedik. Gürültülü
> alarm akışından başlayarak olayın ilk tetikleyicisini ve servisler
> üzerindeki yayılımını açıklamaya çalışan, deterministic + AI hibrit
> bir AIOps platformu geliştirdik.**

### Neden binlerce alarmı doğrudan LLM'e göndermiyoruz?

-   Determinism ve tekrar üretilebilirlik kaybolabilir.
-   Açıklanabilirlik azalabilir.
-   Context ve maliyet büyür.
-   Root-cause ile semptom ayrımı zorlaşır.
-   Aynı veri üzerinde model cevabına bağımlılık artar.

Bunun yerine:

``` text
Alarm Stream
    ↓
Deterministic Reduction
    ↓
Qualified Evidence
    ↓
Causal Incident Discovery
    ↓
Compact Incident Context
    ↓
Qwen Expert
```

### Demo hikâyesi

``` text
1. Veri paketini yükle
        ↓
2. Tüm alarm akışını işle
        ↓
3. Gürültüyü azalt
        ↓
4. Incident candidate'ları oluştur
        ↓
5. Bir incident seç
        ↓
6. İlk tetikleyiciyi göster
        ↓
7. Dependency propagation'ı göster
        ↓
8. RCA ve önerilen aksiyonu göster
```

------------------------------------------------------------------------

## 5. Operasyonel Değer

> **"Binlerce alarmın arkasındaki gerçek problemi, nerede başladığını,
> nasıl yayıldığını ve ilk ne yapılması gerektiğini göstermek."**

  Problem                         Platform Yaklaşımı
  ------------------------------- ----------------------------------------
  Alarm bombardımanı              Qualification + noise suppression
  Aynı olayın yüzlerce semptomu   Template + aggregation
  Yanlış alarm korelasyonu        Time + topology + dependency
  Root ile semptomun karışması    Earliest-trigger causal reasoning
  Kara kutu AI sonucu             XAI / evidence trail
  LLM hallucination riski         Deterministic incident discovery
  RCA'dan sonra ne yapılacağı     Action recommendation + owner + status

------------------------------------------------------------------------

## 6. Çalıştırma

``` bash
streamlit run src/frontend/streamlit_app.py
```

Causal doğrulama:

``` bash
python tools/first_trigger_causal_validator.py data.zip
```

Tam kanıt zinciri:

``` bash
python tools/first_trigger_causal_validator.py data.zip --json causal_validation.json
```

------------------------------------------------------------------------

## 7. Öne Çıkan Kanıt Dosyaları

``` text
src/
├── backend/
│   ├── segmentation_layer/
│   ├── parser_layer/
│   ├── template_layer/
│   ├── aggregation_layer/
│   ├── signal_qualification_layer/
│   ├── correlation_layer/
│   ├── incident_candidate_layer/
│   ├── context_enrichment_layer/
│   ├── rca_layer/
│   ├── learning_planning_layer/
│   ├── input_package_layer/
│   ├── prompts/
│   ├── ai_engine.py
│   ├── prompt_registry.py
│   ├── downstream_pipeline.py
│   └── full_pipeline_v2.py
└── frontend/
    └── streamlit_app.py

tools/
└── first_trigger_causal_validator.py

docs/
├── plan.md
└── mimari.md

prompts/
CLAUDE.md
conv.history.md
```

> **Not:** Kanıt yolları teslim öncesinde repository'deki gerçek dosya
> adlarıyla son kez doğrulanmalıdır.

------------------------------------------------------------------------

## 8. Tek Cümlede Projemiz

> **teletabiler-ai-ops; binlerce log ve alarmı yalnızca özetlemek
> yerine, gürültüyü azaltan, zaman ve servis bağımlılıkları üzerinden
> olayları ilişkilendiren, ilk tetikleyiciyi ve yayılım zincirini
> açıklamaya çalışan ve elde edilen kanıtı AI uzmanıyla RCA ve aksiyona
> dönüştüren açıklanabilir AIOps platformudur.**
