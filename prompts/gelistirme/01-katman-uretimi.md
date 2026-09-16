# Kalıp 01 — Yeni pipeline katmanı üretimi

**Ne zaman:** 9 katmanlı hattın bir halkası yazılacak veya değiştirilecekse.

**Neden bu kalıp:** Katman sözleşmesi (girdi/çıktı şekli + kanıt alanı)
önceden yazılmazsa AI asistanı "çalışan ama açıklanamayan" kod üretir.
Bu kalıp kanıt üretimini isteğe bağlı olmaktan çıkarır.

---

## Kalıp

```
Bağlam: AOP pipeline'ında <KATMAN ADI> katmanını yazacaksın.
CLAUDE.md'deki deterministik çekirdek ilkesi geçerli.

Sözleşme:
- Girdi:  <tam veri şekli — hangi alanlar garanti, hangileri opsiyonel>
- Çıktı:  <tam veri şekli>
- Bu katman DETERMİNİSTİKTİR. LLM çağrısı YAPMA.
- Her çıktı kaydı bir `<evidence_alan_adi>` listesi taşımak ZORUNDA.
  Bu liste, o kaydın neden bu puanı/kararı aldığını Türkçe etiketlerle
  açıklar. Puan tek başına yeterli değildir.

Kısıtlar:
- Girdiyi tamamen RAM'e alma; iterable olarak tüket.
- Bilinmeyen/eksik alanlar için varsayılan uydurma; 'unknown' kullan ve
  bunu kanıt listesinde belirt.
- Eşik ve ağırlık değerlerini modül seviyesinde adlandırılmış sabit yap;
  fonksiyon gövdesine gömme.

Bana önce şunu ver:
1. Kararın hangi kanıt ailelerinden oluştuğunu gösteren bir tablo
   (kanıt | puan etkisi | koşul)
2. Bu tabloyu uygulayan kod

Sonra şu üç soruyu cevapla:
- Bu katman hangi girdide YANLIŞ karar verir?
- Aynı girdi iki kez verilirse çıktı birebir aynı mı? Neden?
- Bir sonraki katman bu çıktının hangi alanına güveniyor?
```

---

## Neden işe yaradı

- **"Kanıt listesi zorunlu"** maddesi, `qualification_evidence`,
  `correlation.evidence`, `root_cause_candidates[].evidence` alanlarının
  baştan var olmasını sağladı. Sonradan XAI eklemek zorunda kalmadık.
- **"Önce tablo, sonra kod"** talebi, puanlama mantığını koddan önce
  gözden geçirilebilir hale getirdi. Gürültü kapısının üç semantik sınıfı
  bu tabloda tartışılıp onaylandı.
- **"Hangi girdide yanlış karar verir?"** sorusu,
  `BACKGROUND_TYPES` için ayrı ve daha sert kuralın gerekliliğini ortaya
  çıkardı ([noise_gate.py:44-46](../../src/backend/signal_qualification_layer/noise_gate.py#L44-L46)).

## İnsan kararı olarak kalan kısım

Kanıt ailelerinin **hangileri olacağı** ve **ağırlıkları** insan tarafından
belirlendi. AI asistanı tabloyu uyguladı, tabloyu yazmadı.
