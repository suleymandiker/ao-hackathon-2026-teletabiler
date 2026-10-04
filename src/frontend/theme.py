"""AI-IN-AI visual primitives. All dynamic HTML content is escaped here."""
from html import escape


CSS = """
<style>
:root { --ai-ink:#17243b; --ai-muted:#61728d; --ai-line:#e1e8f2; --ai-blue:#0764e8; }
[data-testid="stAppViewContainer"] { background:#f6f8fc; }
[data-testid="stHeader"] { background:#f6f8fcee; }
.block-container { max-width:1480px; padding:2.3rem 2.6rem 3rem; }
[data-testid="stSidebar"] { width:224px; min-width:224px; background:#0c1929; border-right:1px solid #172b43; }
[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] { padding:1.5rem 1rem; }
[data-testid="stSidebar"] [data-testid="stImage"] img { border-radius:6px; }
[data-testid="stSidebar"] [role="radiogroup"] { gap:.35rem; }
[data-testid="stSidebar"] [data-testid="stRadio"] label { padding:.45rem .4rem; border-radius:6px; }
[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) { background:#174b87; }
h1 { font-size:1.9rem!important; letter-spacing:-.045rem; font-weight:700!important; }
h2 { font-size:1.2rem!important; letter-spacing:-.015rem; }
h3 { font-size:1.02rem!important; }
[data-testid="stVerticalBlockBorderWrapper"] { border-radius:9px; }
[data-testid="stDataFrame"] { border:1px solid var(--ai-line); border-radius:8px; }
.stButton button { border-radius:6px; font-weight:600; }
.ai-header { display:flex; justify-content:space-between; align-items:center; margin-bottom:1rem; color:var(--ai-muted); font-size:.78rem; }
.ai-status { display:inline-flex; align-items:center; gap:7px; }
.ai-status::before { content:''; width:6px; height:6px; border-radius:50%; background:#0764e8; }
.ai-intro { color:var(--ai-muted); font-size:.94rem; margin:-.4rem 0 1.35rem; max-width:76ch; }
.ai-task-icon { width:38px; height:38px; background:#eaf2ff; border-radius:8px; color:#0764e8; display:flex; align-items:center; justify-content:center; font-size:1.25rem; margin-bottom:.8rem; }
.ai-task-copy { min-height:54px; color:var(--ai-muted); font-size:.88rem; line-height:1.65; margin:.2rem 0 1.1rem; }
.ai-summary { border:1px solid #bde4d5; background:#effaf5; border-radius:9px; padding:1.25rem 1.45rem; margin:.6rem 0 1.2rem; }
.ai-summary--danger { background:#fff2f3; border-color:#f3c8cf; }
.ai-summary--attention { background:#fffaed; border-color:#eeddb6; }
.ai-summary--info { background:#eff5ff; border-color:#cadaf5; }
.ai-eyebrow { font-size:.76rem; font-weight:600; margin-bottom:.5rem; color:#16845a; }
.ai-summary--danger .ai-eyebrow { color:#bd3249; }
.ai-summary--attention .ai-eyebrow { color:#a76806; }
.ai-summary h2 { color:var(--ai-ink); margin:0 0 .5rem; padding:0; font-size:1.3rem!important; }
.ai-summary p { color:var(--ai-muted); font-size:.87rem; margin:0; }
.ai-counts { display:flex; gap:.55rem; flex-wrap:wrap; margin:.75rem 0; font-size:.84rem; color:#344d6d; }
.ai-counts span+span::before { content:'→'; margin-right:.55rem; color:#8fa1b9; }
.ai-flow { display:grid; grid-template-columns:repeat(8,minmax(98px,1fr)); gap:.6rem; overflow-x:auto; padding:.3rem 0 .8rem; }
.ai-stage { border:1px solid var(--ai-line); border-radius:7px; background:white; padding:.8rem .4rem; text-align:center; min-height:114px; position:relative; }
.ai-stage:not(:last-child)::after { content:'→'; position:absolute; right:-.52rem; top:43%; color:#9cacbe; font-size:.7rem; z-index:1; }
.ai-stage__name { font-size:.73rem; font-weight:600; color:#344865; }
.ai-stage__value { font-size:1.15rem; font-weight:650; margin:.55rem 0 .4rem; color:var(--ai-ink); }
.ai-stage__status { font-size:.7rem; color:#16845a; }
.ai-stage--success { border-color:#cce9dd; }
.ai-stage--attention { background:#fffcf3; border-color:#f1dbae; }
.ai-stage--attention .ai-stage__status { color:#ae720a; }
.ai-stage--inactive { background:#f8fafc; }
.ai-stage--inactive .ai-stage__status { color:#8b9ab0; }
.ai-empty { border:1px dashed #d4dfec; border-radius:8px; padding:2rem; color:var(--ai-muted); text-align:center; margin:.75rem 0; background:#fff; }
.ai-empty strong { display:block; color:var(--ai-ink); margin-bottom:.55rem; font-size:1rem; }
.ai-badge { display:inline-block; background:#edf2f9; color:#48617f; border-radius:4px; padding:.2rem .45rem; font-size:.72rem; font-weight:600; }
.ai-badge--danger { background:#ffebee; color:#b8233c; }
.ai-badge--attention { background:#fff2d9; color:#a56906; }
.ai-badge--success { background:#e6f5ee; color:#147b54; }
.ai-pattern { font-family:ui-monospace,Consolas,monospace; font-size:.88rem; overflow-wrap:anywhere; background:#f3f6fc; border-radius:6px; padding:.85rem; margin:.65rem 0; white-space:pre-wrap; }
.ai-note { color:var(--ai-muted); font-size:.78rem; line-height:1.6; }
@media(max-width:1000px) { .block-container { padding:1.5rem; } .ai-flow { grid-template-columns:repeat(8,108px); } }
</style>
"""


def e(value):
    return escape(str(value), quote=True)


def header(breadcrumb, status):
    return f'<div class="ai-header"><span>{e(breadcrumb)}</span><span class="ai-status">{e(status)}</span></div>'


def intro(text):
    return f'<p class="ai-intro">{e(text)}</p>'


def task(icon, title, description):
    return f'<div class="ai-task-icon">{e(icon)}</div><h3>{e(title)}</h3><div class="ai-task-copy">{e(description)}</div>'


def summary(view):
    counts = ''.join(f'<span>{e(value)} {e(label)}</span>' for label, value in view.counts)
    tone = view.tone if view.tone in ('success', 'attention', 'danger', 'info') else 'info'
    return (f'<section class="ai-summary ai-summary--{tone}"><div class="ai-eyebrow">✓ Analiz tamamlandı</div>'
            f'<h2>{e(view.title)}</h2><div class="ai-counts">{counts}</div><p>{e(view.explanation)}</p></section>')


def pipeline(stages):
    labels = {'success': '✓ Çıktı üretildi', 'attention': '◇ Dikkat', 'inactive': '—'}
    return '<div class="ai-flow">' + ''.join(
        f'<div class="ai-stage ai-stage--{stage.status}"><div class="ai-stage__name">{e(stage.name)}</div>'
        f'<div class="ai-stage__value">{e(stage.value)}</div><div class="ai-stage__status">{labels[stage.status]}</div></div>'
        for stage in stages) + '</div>'


def empty(title, description):
    return f'<div class="ai-empty"><strong>{e(title)}</strong>{e(description)}</div>'


def badge(label, tone='info'):
    tone = tone if tone in ('success', 'attention', 'danger', 'info') else 'info'
    return f'<span class="ai-badge ai-badge--{tone}">{e(label)}</span>'


def pattern(text):
    return f'<div class="ai-pattern">{e(text)}</div>'
