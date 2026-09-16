"""AOP tasarım sistemi — Turkcell kurumsal kimliği.

Tek kaynak: renk token'ları, global CSS ve yeniden kullanılabilir HTML bileşenleri.
Arayüz mantığı burada yok; yalnız sunum katmanı.
"""
from __future__ import annotations

from html import escape

# --- Marka token'ları -------------------------------------------------------
TC_YELLOW = "#FFC800"      # Turkcell Sarısı
TC_NAVY = "#0A1628"        # Turkcell Lacivert (zemin)
TC_BLUE = "#2E86DE"
TC_TEAL = "#00C389"
TC_RED = "#FF4D6A"
TC_ORANGE = "#FF9F1C"

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

:root{
  --tc-yellow:#FFC800;
  --tc-yellow-700:#E0AF00;
  --tc-yellow-dim:rgba(255,200,0,.13);
  --tc-yellow-line:rgba(255,200,0,.32);
  --navy-900:#050D1A;
  --navy-800:#0A1628;
  --navy-700:#101F38;
  --navy-600:#16294A;
  --blue:#2E86DE;
  --teal:#00C389;
  --red:#FF4D6A;
  --orange:#FF9F1C;
  --ink:#E9EFF9;
  --ink-2:#A8B8CE;
  --ink-3:#6F829C;
  --line:rgba(255,255,255,.08);
  --r-xl:20px; --r-lg:16px; --r-md:12px; --r-sm:8px;
  --shadow:0 10px 32px rgba(0,0,0,.34);
}

/* Metin fontu. Streamlit'in ikon fontunu ezmemek için ikon elemanları hariç tutulur:
   aksi halde Material ligature'ları "arrow_drop_down" gibi düz metin olarak basılır. */
html, body, .stMarkdown, .stApp,
[data-testid="stAppViewContainer"], [data-testid="stSidebar"],
[class*="st-"]:not([class*="material"]),
input, button, textarea, select{
  font-family:'Inter',-apple-system,'Segoe UI',Roboto,sans-serif;
}
[data-testid="stIconMaterial"],
[data-testid="stExpanderIcon"],
.material-icons, .material-icons-outlined,
[class*="material-symbols"], [class*="material-icons"]{
  font-family:'Material Symbols Rounded','Material Icons'!important;
  font-feature-settings:'liga'!important;
  -webkit-font-feature-settings:'liga'!important;
}

[data-testid="stAppViewContainer"]{
  background:
    radial-gradient(1100px 520px at 12% -10%, rgba(255,200,0,.10), transparent 62%),
    radial-gradient(900px 480px at 92% 0%, rgba(46,134,222,.12), transparent 60%),
    var(--navy-800);
}
[data-testid="stHeader"]{background:transparent;}
.block-container{padding-top:1.4rem;padding-bottom:4rem;max-width:1560px;}

/* ---------- Kenar çubuğu ---------- */
[data-testid="stSidebar"]{
  background:linear-gradient(180deg,#070F1E 0%,#0A1628 100%);
  border-right:1px solid var(--line);
}
[data-testid="stSidebar"] .block-container{padding-top:1.5rem;}

.brand{display:flex;align-items:center;gap:.7rem;margin-bottom:.35rem;}
.brand__mark{
  width:40px;height:40px;border-radius:11px;flex:0 0 40px;
  background:linear-gradient(140deg,var(--tc-yellow),#FFDE59);
  display:flex;align-items:center;justify-content:center;
  font-weight:800;font-size:1.05rem;color:#09182C;
  box-shadow:0 6px 18px rgba(255,200,0,.28);
}
.brand__name{font-size:1.12rem;font-weight:800;letter-spacing:-.02em;color:var(--ink);line-height:1.1;}
.brand__sub{font-size:.68rem;font-weight:600;letter-spacing:.16em;text-transform:uppercase;color:var(--ink-3);}

.side-label{
  font-size:.66rem;font-weight:700;letter-spacing:.16em;text-transform:uppercase;
  color:var(--ink-3);margin:1.15rem 0 .4rem;
}

/* ---------- Kahraman alanı ---------- */
.hero{
  position:relative;overflow:hidden;
  padding:1.7rem 1.9rem;margin-bottom:1.5rem;
  border:1px solid var(--line);border-radius:var(--r-xl);
  background:
    radial-gradient(620px 240px at 88% -40%, rgba(255,200,0,.18), transparent 70%),
    linear-gradient(115deg, #0C1D36 0%, #0A1628 55%, #0B1F3A 100%);
  box-shadow:var(--shadow);
}
.hero::before{
  content:"";position:absolute;left:0;top:0;bottom:0;width:5px;
  background:linear-gradient(180deg,var(--tc-yellow),rgba(255,200,0,.15));
}
.hero__eyebrow{
  display:inline-flex;align-items:center;gap:.45rem;
  font-size:.68rem;font-weight:700;letter-spacing:.2em;text-transform:uppercase;
  color:var(--tc-yellow);margin-bottom:.55rem;
}
.hero__eyebrow::before{content:"";width:22px;height:2px;background:var(--tc-yellow);border-radius:2px;}
.hero__title{
  font-size:2.05rem;font-weight:800;letter-spacing:-.035em;line-height:1.1;
  color:var(--ink);margin:0 0 .55rem;
}
.hero__title em{font-style:normal;color:var(--tc-yellow);}
.hero__text{font-size:.95rem;line-height:1.6;color:var(--ink-2);max-width:78ch;margin:0;}
.hero__pills{display:flex;flex-wrap:wrap;gap:.45rem;margin-top:1rem;}

/* ---------- Bölüm başlığı ---------- */
.section{margin:2.1rem 0 .85rem;}
.section__title{
  display:flex;align-items:center;gap:.6rem;
  font-size:1.14rem;font-weight:700;letter-spacing:-.015em;color:var(--ink);margin:0;
}
.section__title::before{content:"";width:4px;height:19px;border-radius:3px;background:var(--tc-yellow);}
.section__sub{font-size:.85rem;color:var(--ink-3);margin:.35rem 0 0 1rem;line-height:1.5;}

/* ---------- KPI kartları ---------- */
.kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(168px,1fr));gap:.75rem;}
.kpi{
  position:relative;overflow:hidden;
  padding:.95rem 1.05rem;border:1px solid var(--line);border-radius:var(--r-lg);
  background:linear-gradient(160deg,rgba(255,255,255,.045),rgba(255,255,255,.012));
  transition:border-color .18s ease, transform .18s ease;
}
.kpi:hover{border-color:var(--tc-yellow-line);transform:translateY(-2px);}
.kpi::after{content:"";position:absolute;left:0;top:0;height:3px;width:100%;background:var(--tc-yellow);opacity:.85;}
.kpi--muted::after{background:var(--ink-3);opacity:.5;}
.kpi--good::after{background:var(--teal);}
.kpi--warn::after{background:var(--orange);}
.kpi--bad::after{background:var(--red);}
.kpi--info::after{background:var(--blue);}
.kpi__label{font-size:.7rem;font-weight:700;letter-spacing:.11em;text-transform:uppercase;color:var(--ink-3);}
.kpi__value{font-size:1.85rem;font-weight:800;letter-spacing:-.03em;color:var(--ink);line-height:1.15;margin-top:.3rem;}
.kpi__hint{font-size:.76rem;color:var(--ink-3);margin-top:.2rem;line-height:1.4;}

/* ---------- İşlem hattı adımları ---------- */
.flow{display:flex;align-items:stretch;gap:0;flex-wrap:nowrap;overflow-x:auto;padding:.2rem 0 .5rem;}
.flow__step{
  position:relative;flex:1 1 0;min-width:112px;
  padding:.85rem .6rem;text-align:center;
  border:1px solid var(--line);border-radius:var(--r-md);
  background:rgba(255,255,255,.022);margin-right:.42rem;
}
.flow__step:last-child{margin-right:0;}
.flow__step--done{border-color:rgba(0,195,137,.30);background:rgba(0,195,137,.055);}
.flow__step--active{
  border-color:var(--tc-yellow);background:var(--tc-yellow-dim);
  box-shadow:0 0 0 1px var(--tc-yellow-line), 0 8px 22px rgba(255,200,0,.14);
}
.flow__dot{font-size:.72rem;line-height:1;}
.flow__step--done .flow__dot{color:var(--teal);}
.flow__step--active .flow__dot{color:var(--tc-yellow);}
.flow__step--todo .flow__dot{color:var(--ink-3);}
.flow__name{font-size:.72rem;font-weight:700;color:var(--ink-2);margin-top:.35rem;letter-spacing:-.005em;}
.flow__step--active .flow__name{color:var(--tc-yellow);}
.flow__value{font-size:1.32rem;font-weight:800;color:var(--ink);letter-spacing:-.025em;margin-top:.15rem;}
.flow__unit{font-size:.66rem;color:var(--ink-3);letter-spacing:.06em;text-transform:uppercase;}

/* ---------- İzleme kartları (Girdi → Karar → Çıktı) ---------- */
.trace{
  height:100%;min-height:196px;padding:1.15rem 1.2rem;
  border:1px solid var(--line);border-radius:var(--r-lg);
  background:linear-gradient(165deg,rgba(255,255,255,.045),rgba(255,255,255,.01));
}
.trace--decision{border-color:var(--tc-yellow-line);background:linear-gradient(165deg,rgba(255,200,0,.10),rgba(255,200,0,.02));}
.trace__title{font-size:.67rem;font-weight:800;letter-spacing:.17em;text-transform:uppercase;color:var(--ink-3);margin-bottom:.6rem;}
.trace--decision .trace__title{color:var(--tc-yellow);}
.trace__value{font-size:1.65rem;font-weight:800;letter-spacing:-.03em;color:var(--ink);line-height:1.15;}
.trace__lead{display:block;font-size:.88rem;font-weight:700;color:var(--ink-2);margin:.3rem 0 .45rem;}
.trace__text{font-size:.85rem;line-height:1.55;color:var(--ink-3);}
.trace__arrow{display:flex;align-items:center;justify-content:center;height:196px;font-size:1.3rem;color:var(--tc-yellow);opacity:.6;}

/* ---------- Genel kart ---------- */
.card{
  padding:1.1rem 1.2rem;border:1px solid var(--line);border-radius:var(--r-lg);
  background:linear-gradient(165deg,rgba(255,255,255,.04),rgba(255,255,255,.01));
}
.card--accent{border-left:3px solid var(--tc-yellow);}
.card__title{font-size:.68rem;font-weight:800;letter-spacing:.15em;text-transform:uppercase;color:var(--ink-3);margin-bottom:.7rem;}
.card__row{font-size:.9rem;color:var(--ink-2);line-height:1.85;}
.card__row b{color:var(--ink);font-weight:600;}
.card__mono{
  font-family:ui-monospace,'SF Mono',Menlo,Consolas,monospace;font-size:.86rem;
  color:var(--ink);background:rgba(0,0,0,.28);border:1px solid var(--line);
  border-radius:var(--r-sm);padding:.6rem .7rem;margin:.2rem 0 .8rem;word-break:break-word;
}

/* ---------- Rozet / çip ---------- */
.chip{
  display:inline-block;padding:.24rem .62rem;margin:.16rem .2rem .16rem 0;
  border-radius:999px;font-size:.75rem;font-weight:600;
  background:var(--tc-yellow-dim);border:1px solid var(--tc-yellow-line);color:#FFDC6B;
}
.pill{
  display:inline-flex;align-items:center;gap:.4rem;margin:.16rem .25rem .16rem 0;
  padding:.3rem .7rem;border-radius:999px;font-size:.74rem;font-weight:600;
  background:rgba(255,255,255,.06);border:1px solid var(--line);color:var(--ink-2);
}
.pill__dot{width:7px;height:7px;border-radius:50%;background:var(--ink-3);}
.pill--muted{opacity:.85;}
.pill--yellow{background:var(--tc-yellow-dim);border-color:var(--tc-yellow-line);color:#FFDC6B;}
.pill--yellow .pill__dot{background:var(--tc-yellow);}
.pill--good{background:rgba(0,195,137,.12);border-color:rgba(0,195,137,.30);color:#6FE3BE;}
.pill--good .pill__dot{background:var(--teal);}
.pill--warn{background:rgba(255,159,28,.12);border-color:rgba(255,159,28,.30);color:#FFC680;}
.pill--warn .pill__dot{background:var(--orange);}
.pill--bad{background:rgba(255,77,106,.12);border-color:rgba(255,77,106,.30);color:#FF97AB;}
.pill--bad .pill__dot{background:var(--red);}
.pill--info{background:rgba(46,134,222,.12);border-color:rgba(46,134,222,.30);color:#8CC3F5;}
.pill--info .pill__dot{background:var(--blue);}

/* ---------- Güven ölçeri ---------- */
.meter{margin-top:.55rem;}
.meter__track{height:6px;border-radius:99px;background:rgba(255,255,255,.08);overflow:hidden;}
.meter__fill{height:100%;border-radius:99px;background:linear-gradient(90deg,var(--tc-yellow-700),var(--tc-yellow));}
.meter__label{font-size:.72rem;color:var(--ink-3);margin-top:.3rem;}

/* ---------- Boş durum ---------- */
.empty{
  padding:1.6rem;text-align:center;border:1px dashed var(--line);border-radius:var(--r-lg);
  background:rgba(255,255,255,.015);color:var(--ink-3);font-size:.9rem;line-height:1.6;
}
.empty b{display:block;color:var(--ink-2);font-size:1rem;margin-bottom:.35rem;}

/* ---------- Streamlit bileşen düzeltmeleri ---------- */
.stButton>button[kind="primary"]{
  background:linear-gradient(140deg,var(--tc-yellow),#FFD84D);
  color:#09182C;font-weight:700;border:none;border-radius:var(--r-md);
  padding:.55rem 1rem;box-shadow:0 6px 18px rgba(255,200,0,.24);transition:all .18s ease;
}
.stButton>button[kind="primary"]:hover{filter:brightness(1.06);transform:translateY(-1px);box-shadow:0 9px 24px rgba(255,200,0,.32);}

[data-testid="stMetric"]{
  padding:.85rem 1rem;border:1px solid var(--line);border-radius:var(--r-lg);
  background:linear-gradient(160deg,rgba(255,255,255,.04),rgba(255,255,255,.01));
}
[data-testid="stMetricLabel"] p{
  font-size:.7rem!important;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--ink-3)!important;
}
[data-testid="stMetricValue"]{font-size:1.6rem!important;font-weight:800;letter-spacing:-.03em;color:var(--ink);}

.stTabs [data-baseweb="tab-list"]{gap:.15rem;border-bottom:1px solid var(--line);}
.stTabs [data-baseweb="tab"]{
  height:42px;padding:0 1rem;font-size:.88rem;font-weight:600;color:var(--ink-3);
  background:transparent;border-radius:var(--r-sm) var(--r-sm) 0 0;
}
.stTabs [data-baseweb="tab"]:hover{color:var(--ink-2);background:rgba(255,255,255,.03);}
.stTabs [aria-selected="true"]{color:var(--tc-yellow)!important;background:var(--tc-yellow-dim)!important;}
.stTabs [data-baseweb="tab-highlight"]{background:var(--tc-yellow);height:2px;}

[data-testid="stExpander"]{border:1px solid var(--line);border-radius:var(--r-md);background:rgba(255,255,255,.02);}
[data-testid="stExpander"] summary:hover{color:var(--tc-yellow);}

[data-testid="stFileUploaderDropzone"]{
  background:rgba(255,255,255,.025);border:1px dashed rgba(255,200,0,.28);border-radius:var(--r-md);
}
[data-testid="stFileUploaderDropzone"]:hover{border-color:var(--tc-yellow);background:var(--tc-yellow-dim);}

[data-testid="stDataFrame"]{border:1px solid var(--line);border-radius:var(--r-md);overflow:hidden;}
[data-testid="stElementToolbar"]{background:var(--navy-700);}

hr{border-color:var(--line)!important;}
::-webkit-scrollbar{width:9px;height:9px;}
::-webkit-scrollbar-track{background:transparent;}
::-webkit-scrollbar-thumb{background:rgba(255,255,255,.12);border-radius:99px;}
::-webkit-scrollbar-thumb:hover{background:var(--tc-yellow-line);}
</style>
"""


# --- HTML bileşenleri -------------------------------------------------------
def _e(value) -> str:
    return escape(str(value), quote=True)


def hero(eyebrow: str, title_html: str, text: str, pills: list[tuple[str, str]] | None = None) -> str:
    """Sayfa başlığı. `title_html` içinde <em> vurgusu kullanılabilir."""
    pill_html = "".join(pill(label, tone) for label, tone in (pills or []))
    return (
        f'<div class="hero"><div class="hero__eyebrow">{_e(eyebrow)}</div>'
        f'<h1 class="hero__title">{title_html}</h1>'
        f'<p class="hero__text">{_e(text)}</p>'
        f'<div class="hero__pills">{pill_html}</div></div>'
    )


def section(title: str, subtitle: str = "") -> str:
    sub = f'<div class="section__sub">{_e(subtitle)}</div>' if subtitle else ""
    return f'<div class="section"><h2 class="section__title">{_e(title)}</h2>{sub}</div>'


def pill(label: str, tone: str = "base") -> str:
    cls = f" pill--{tone}" if tone != "base" else ""
    return f'<span class="pill{cls}"><span class="pill__dot"></span>{_e(label)}</span>'


def kpi(label: str, value, hint: str = "", tone: str = "base") -> str:
    cls = f" kpi--{tone}" if tone != "base" else ""
    hint_html = f'<div class="kpi__hint">{_e(hint)}</div>' if hint else ""
    return (
        f'<div class="kpi{cls}"><div class="kpi__label">{_e(label)}</div>'
        f'<div class="kpi__value">{_e(value)}</div>{hint_html}</div>'
    )


def kpi_grid(cards: list[str]) -> str:
    return f'<div class="kpi-grid">{"".join(cards)}</div>'


def flow(steps: list[tuple[str, object, str]], active_index: int | None = None) -> str:
    """Yatay işlem hattı. active_index None ise tüm adımlar tamamlanmış sayılır."""
    out = []
    for idx, (name, value, unit) in enumerate(steps):
        if active_index is None or idx < active_index:
            state, dot = "done", "✓"
        elif idx == active_index:
            state, dot = "active", "●"
        else:
            state, dot = "todo", "○"
        out.append(
            f'<div class="flow__step flow__step--{state}">'
            f'<div class="flow__dot">{dot}</div>'
            f'<div class="flow__name">{_e(name)}</div>'
            f'<div class="flow__value">{_e(value)}</div>'
            f'<div class="flow__unit">{_e(unit)}</div></div>'
        )
    return f'<div class="flow">{"".join(out)}</div>'


def trace_card(title: str, value, lead: str, text: str, kind: str = "") -> str:
    cls = " trace--decision" if kind == "decision" else ""
    return (
        f'<div class="trace{cls}"><div class="trace__title">{_e(title)}</div>'
        f'<div class="trace__value">{_e(value)}</div>'
        f'<span class="trace__lead">{_e(lead)}</span>'
        f'<div class="trace__text">{_e(text)}</div></div>'
    )


def trace_arrow() -> str:
    return '<div class="trace__arrow">→</div>'


def chips(items) -> str:
    rendered = "".join(f'<span class="chip">{_e(str(x).replace("_", " "))}</span>' for x in (items or []))
    return rendered or '<span class="pill">Kanıt yok</span>'


def meter(ratio: float, label: str = "") -> str:
    pct = max(0.0, min(1.0, float(ratio))) * 100
    lbl = f'<div class="meter__label">{_e(label)}</div>' if label else ""
    return f'<div class="meter"><div class="meter__track"><div class="meter__fill" style="width:{pct:.0f}%"></div></div>{lbl}</div>'


def empty_state(title: str, text: str) -> str:
    return f'<div class="empty"><b>{_e(title)}</b>{_e(text)}</div>'


def brand_block() -> str:
    return (
        '<div class="brand"><div class="brand__mark">◈</div>'
        '<div><div class="brand__name">AOP</div>'
        '<div class="brand__sub">Turkcell SRE</div></div></div>'
    )


def side_label(text: str) -> str:
    return f'<div class="side-label">{_e(text)}</div>'
