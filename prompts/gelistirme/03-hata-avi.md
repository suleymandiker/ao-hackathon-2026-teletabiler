# Kalıp 03 — Deterministik katmanlarda mantık hatası avı

**Ne zaman:** Bir katman "çalışıyor" ama sonuçlara güvenmiyorsanız.

**Neden bu kalıp:** Deterministik kodun tehlikesi, yanlış olduğunda da
kararlı biçimde yanlış olmasıdır. Test yoksa hata fark edilmez. Bu kalıp
AI asistanını "kodu savun" yerine **"kodu çürüt"** moduna sokar.

---

## Kalıp

```
Bağlam: <dosya> içindeki <sınıf/fonksiyon> deterministik bir karar veriyor.
Bu kodu SAVUNMA. ÇÜRÜTMEYE çalış.

Görev:
1. Bu fonksiyonun ÜRETTİĞİ çıktının YANLIŞ olacağı somut girdiler kurgula.
   Her senaryo için:
     - somut girdi (gerçekçi alan değerleriyle)
     - kodun üreteceği çıktı
     - doğru çıktının ne olması gerektiği
     - hatanın operasyonel sonucu (operatör ne yanlış yapar?)

2. Özellikle şunları ara:
   - İki BAĞIMSIZ olgunun tek bir sonuca çökmesi
   - Bir sonucun yanlış BÖLÜNMESİ
   - Sıralama/tie-break belirsizliği (aynı skorlu iki aday)
   - Boş koleksiyon, tek elemanlı koleksiyon, tümü aynı değer
   - Zaman: eşit timestamp, ters sıra, eksik timestamp
   - Birleştirme (union-find / graph) mantığında geçişlilik tuzağı

3. En ciddi bulguyu seç ve DÜZELTME ÖNERİSİ ver.
   Öneri mevcut davranışı bozuyorsa, hangi senaryoların değişeceğini söyle.

Kural: "Bir hata bulamadım" cevabı kabul değil. En az 3 senaryo üret ve
her birini kodda izleyerek doğrula.
```

---

## Bu kalıbın yakaladığı en önemli hata

**Bulgu:** Olay üretimi korelasyon grafiğinin **bağlı bileşenlerini**
kullanıyordu.

**Kurgulanan girdi:** İki tamamen bağımsız kök neden — `DC1/R12`'de bir ağ
arızası ve `DC2`'de bir DB disk dolması. İkisi de kendi bağımlı servislerinde
`timeout` semptomu üretiyor. Bu iki `timeout` sinyali aynı pencerede ve aynı
şablon ailesinde olduğu için birbirine bağlanıyor.

**Kodun ürettiği:** Tek bir dev olay. Kök neden: ikisinden rastgele biri.

**Doğrusu:** İki ayrı olay, iki ayrı kök neden, iki ayrı sahip ekibi.

**Operasyonel sonuç:** Operatör ağ ekibini çağırır, DB arızası saatlerce
devam eder.

**Düzeltme:** Root-seeded kümeleme
([builder.py:46-99](../../src/backend/incident_candidate_layer/builder.py#L46-L99)) —
yalnız `ROOT_TYPES` çekirdek olabilir, semptomlar **en iyi tek çekirdeğe**
iliştirilir ve asla iki çekirdeği birleştiremez.

**Regresyon kontrolü:** `tools/benchmark.py` içindeki K3 kontrolü
("tek nedensel zincir tek olay") bu davranışı kilitler.

---

## Diğer bulgular

| Bulgu | Düzeltme |
|---|---|
| Aynı servisin ilgisiz semptomları (`timeout` + `cert_expiry`) tek sinyalde çöküyordu | `alarm_type` gruplama anahtarına eklendi — [aggregator.py:117-122](../../src/backend/aggregation_layer/aggregator.py#L117-L122) |
| Tek yüksek-severity satır 30 satırlık `cpu_high` gürültüsünü geçiriyordu | Arka plan için ayrı kural: `count>=10 AND source_severity>=4` |
| Dosya büyüdükçe stratified örnek kayıyor, aynı format "yeni format" sayılıyordu | İmza örneklemesi keşif örneklemesinden ayrıldı — [segmentation_pipeline.py:136-141](../../src/backend/segmentation_layer/segmentation_pipeline.py#L136-L141) |
| DB olayında kök, bağımlılığın hedefi değil kaynağı seçilebiliyordu | Topolojik hedef tercih edilir — [builder.py:88-91](../../src/backend/incident_candidate_layer/builder.py#L88-L91) |
