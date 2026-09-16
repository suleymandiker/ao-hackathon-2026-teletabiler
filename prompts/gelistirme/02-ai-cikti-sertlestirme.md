# Kalıp 02 — LLM entegrasyonunu sertleştirme

**Ne zaman:** Bir LLM çağrısı çalışıyor ama henüz üretime hazır değilse.

**Neden bu kalıp:** "Mutlu yol"da çalışan bir LLM entegrasyonu, üretimde
sessizce yanlış sonuç gösterir. Bu kalıp, modelin başarısız olabileceği
yolları önce listeletir, sonra her biri için kapı yazdırır.

---

## Kalıp

```
Bağlam: <dosya>:<fonksiyon> bir LLM çağrısı yapıyor ve çıktısını kullanıyor.
Bu entegrasyonu üretime hazır hale getir.

Adım 1 — Önce KOD YAZMA. Şu soruyu cevapla:
  Bu model çağrısı kaç farklı şekilde "başarılı görünüp yanlış" olabilir?
  En az 6 senaryo listele. Şunları mutlaka değerlendir:
    - gateway 4xx/5xx döndürdü ama gövde geçerli JSON
    - yanıt token limitinde kesildi (finish_reason=length)
    - model markdown kod çiti içinde JSON döndürdü
    - model doğru şemayı değil, kendi uydurduğu şemayı döndürdü
    - model boş string / null döndürdü
    - model istenen alanı doldurdu ama değer kanıtta yok (halüsinasyon)

Adım 2 — Her senaryo için tek bir kapı yaz. Kapılar sırayla:
  1. sözdizimi (JSON çıkarımı)
  2. şema/semantik (beklenen alanlar gerçekten var mı)
  3. deterministik doğrulama (çıktı gerçek veriye karşı test edilir)

Adım 3 — Fallback zorunlu:
  Hiçbir kapıdan geçmezse sistem ÇALIŞMAYA DEVAM ETMELİ.
  Deterministik sonuç kaybolmamalı. Kullanıcıya AI'ın devre dışı kaldığı
  açıkça gösterilmeli; sessizce boş ekran olmamalı.

Adım 4 — Hata metnini yut ama sakla:
  Exception fırlatma; hatayı `last_*_error` alanına yaz ki arayüz
  gösterebilsin.
```

---

## Bu kalıbın yakaladığı gerçek buglar

| Senaryo | Bulunan davranış | Eklenen kapı |
|---|---|---|
| Gateway hata cevabı da JSON | Hata gövdesi "uzman yorumu" olarak arayüzde gösteriliyordu | Semantik kapı: beklenen alanlardan hiçbiri yoksa reddet — [rca_engine.py:201](../../src/backend/rca_layer/rca_engine.py#L201) |
| `response_format` desteklenmiyor | 400 alınca çağrı komple başarısız oluyordu | Alan çıkarılıp bir kez sade tekrar — [ai_engine.py:152-156](../../src/backend/ai_engine.py#L152-L156) |
| Yanıt token limitinde kesildi | Yarım regex kabul ediliyordu | `finish_reason` kontrolü → deterministik fallback |
| Model markdown çiti ekledi | `json.loads` patlıyordu | Çit temizleme + gövde içinden `{...}` kurtarma — `_extract_json` |
| AI regex logu yanlış bölüyor | Tüm downstream sessizce bozuluyordu | `RegexValidator` ile tüm akışta coverage + parser ölçümü |

## Sonuç

Benchmark koşularının birinde DeepSeek yanıtı gerçekten
`finish_reason=length` ile kesildi. Sistem çökmedi, uyarı yazdı,
deterministik fallback'e düştü ve **aynı 44 mantıksal olayı** üretti.
Bu kalıbın ürettiği değer tam olarak budur.
