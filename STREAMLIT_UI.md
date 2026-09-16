# AOP XAI Streamlit Dashboard V2

Çalıştırma:

```powershell
pip install -r requirements.txt
streamlit run src/frontend/streamlit_app.py
```

> Tema `.streamlit/config.toml` dosyasından okunur, bu yüzden komutu **proje kökünden** çalıştırın.

V2, XAI öncelikli bir olay inceleme çalışma alanı sunar: kök neden hipotezi, nedensellik/korelasyon grafiği,
deterministik karar izi, kanıt defteri, sıralı alternatif hipotezler, operatör-güvenli aksiyon planı ve
açılabilir teknik/ham çıktılar. Panel yalnız deterministik pipeline'ın ürettiği kanıtı gösterir;
LLM/ajan sonucu uydurmaz.

## Tasarım sistemi

Arayüz Turkcell kurumsal kimliğine göre tasarlanmıştır — Turkcell Sarısı (`#FFC800`) vurgu,
lacivert (`#0A1628`) zemin.

| Dosya | Rol |
|---|---|
| [.streamlit/config.toml](.streamlit/config.toml) | Streamlit tema token'ları (renk, kenarlık, yarıçap) |
| [src/frontend/theme.py](src/frontend/theme.py) | Tasarım sistemi: global CSS + yeniden kullanılabilir HTML bileşenleri |
| [src/frontend/streamlit_app.py](src/frontend/streamlit_app.py) | Sayfa mantığı ve veri bağlama |

`theme.py` içindeki bileşenler: `hero`, `section`, `kpi` / `kpi_grid`, `flow` (işlem hattı adımları),
`trace_card` (Girdi → Karar → Çıktı), `card`, `pill`, `chips`, `meter`, `empty_state`.
Tümü HTML kaçışı uygular; log ve LLM kaynaklı metin kartlara güvenle gömülür.

Renk anlamları sabittir: sarı = marka/aktif adım, yeşil = geçti/tamamlandı, turuncu = dikkat,
kırmızı = olay/kök neden, gri = bastırılmış gürültü.

## Arayüz QA

Backend veya LLM çağrısı yapmadan tüm render dallarını (boş durum, Qwen'li/Qwen'siz,
korelasyonlu/korelasyonsuz, 9 katmanın her biri) çalıştırır:

```powershell
python tools/frontend_render_qa.py
```

18 render yolunun tamamı hatasız çalışmalı ve beklenen tasarım markup'ını üretmelidir.
