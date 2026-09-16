# Kalıp 04 — Kanıt gösteren arayüz bileşeni

**Ne zaman:** Pipeline bir sayı üretti ve bunu operatöre göstereceksiniz.

**Neden bu kalıp:** Bir SRE panelinde "19 korelasyon bulundu" yazması bir
şey ifade etmez. Operatörün güvenmesi için **hangi kanıtla** bulunduğunu
görmesi gerekir. Bu kalıp, çıplak sayı göstermeyi yasaklar.

---

## Kalıp

```
Bağlam: <katman> şu çıktıyı üretiyor: <örnek JSON kayıt>
Bu çıktıyı Streamlit arayüzünde göstereceksin.

Kurallar:
1. ÇIPLAK SAYI YASAK. Her sayının yanında şu üçlü olmalı:
     GİRDİ → UYGULANAN KARAR → ÇIKTI
   "Karar" kutusu, o katmanın hangi kurala göre eleme/gruplama yaptığını
   tek cümleyle anlatmalı.

2. Farklı katmanların sayıları AYNI NESNE TÜRÜ DEĞİLDİR.
   "44 olay" ile "40 sinyal adayı" karşılaştırılabilir görünmemeli.
   Her sayının birimi etiketle yazılmalı.

3. Bastırılan/elenen kayıtlar GİZLENMEMELİ.
   Gürültü olarak bastırılan sinyaller de tabloda, "BASTIRILDI" kararı ve
   kanıt etiketleriyle görünmeli. Operatör neyin atıldığını görmeli.

4. GÜVENLİK: Log ve LLM kaynaklı metin HTML'e gömülmeden önce KAÇIŞLANMALI.
   Kart bileşenleri f-string ile HTML üretiyorsa her interpolasyon
   esc() ile sarılmalı.

5. AI çıktısı yoksa boş ekran gösterme. "Qwen çıktısı yok — deterministik
   RCA gösteriliyor" gibi açık bir durum rozeti koy.

6. Renk anlamı sabit olmalı ve tema dosyasında tanımlanmalı:
   sarı = marka/aktif, yeşil = geçti, turuncu = dikkat,
   kırmızı = olay/kök neden, gri = bastırılmış gürültü.
   Bileşen içine hard-coded hex yazma.
```

---

## Ürettiği yapı

**Katman İzleme görünümü** — 9 katmanın her biri için üç kart:

```
┌──────────────┐     ┌────────────────────────┐     ┌──────────────┐
│ GİRDİ        │ ──► │ UYGULANAN KARAR        │ ──► │ ÇIKTI        │
│ 40           │     │ Deterministik gate     │     │ 9            │
│ sinyal adayı │     │ Şiddet+tekrar+burst    │     │ nitelikli    │
│              │     │ +kanıt                 │     │ sinyal       │
└──────────────┘     └────────────────────────┘     └──────────────┘
                   31 aday gürültü olarak bastırıldı
```

Altında o katmanın **gerçek** pipeline çıktısı tablo olarak listelenir.
Jürinin "bu sayı nereden geldi" sorusunun cevabı buradadır.

Kod: [streamlit_app.py:184-293](../../src/frontend/streamlit_app.py#L184-L293)

**Olay XAI sekmeleri** — her olay için 5 sekme: Uzman Case Analizi /
Kök Neden Hipotezi / Korelasyon Kanıtı / Sinyal Kanıtları / Önerilen Aksiyon.
Kod: [streamlit_app.py:397-450](../../src/frontend/streamlit_app.py#L397-L450)

**Tasarım sistemi** — `hero`, `section`, `kpi`, `flow`, `trace_card`,
`card`, `pill`, `chips`, `meter`, `empty_state` bileşenleri tek dosyada:
[theme.py](../../src/frontend/theme.py). Hepsi HTML kaçışı uygular.

## 4. kuralın önemi

Log satırları saldırgan kontrolündedir. `<img src=x onerror=...>` içeren bir
log mesajı, `unsafe_allow_html=True` ile render edilen bir kartta çalışırdı.
`esc()` ([streamlit_app.py:111-113](../../src/frontend/streamlit_app.py#L111-L113))
bunu engeller.
