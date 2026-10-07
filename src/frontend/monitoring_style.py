"""Compact monitor console primitives; dynamic values are escaped at this boundary."""
from theme import e


# Key-scoped selectors leave other application buttons and the sidebar untouched.
# The native button overlays the visual header, retaining keyboard activation and
# an accessible name. Only its painted label is hidden, not the button itself.
CSS = """
<style>
[class*="st-key-monitor_card_"] { padding:0 .85rem!important; background:white; }
[class*="st-key-monitor_header_"] { position:relative; gap:0!important; }
[class*="st-key-monitor_header_"] [data-testid="stMarkdownContainer"] { margin-bottom:0!important; }
[class*="st-key-monitor_header_"] [class*="st-key-monitor_row_"] {
  position:absolute; inset:0; width:100%; height:100%; z-index:2;
}
[class*="st-key-monitor_row_"] button {
  width:100%; height:100%; min-height:4rem; background:transparent!important;
  border:0!important; border-radius:5px; text-align:left; justify-content:flex-start;
}
[class*="st-key-monitor_row_"] button p { opacity:0; }
[class*="st-key-monitor_row_"] button:hover { background:#0764e808!important; }
[class*="st-key-monitor_row_"] button:focus-visible { outline:2px solid var(--ai-blue); outline-offset:-2px; }
[class*="st-key-monitor_card_"]:has(.monitor-row--expanded) { border-color:#9bbce8; }
[class*="st-key-monitor_card_"]:has(.monitor-row--archived) { background:#f1f4f8; }
.monitor-row { display:grid; grid-template-columns:6rem minmax(0,1fr) minmax(10rem,auto) 1rem;
  align-items:center; gap:.8rem; min-height:4.25rem; color:var(--ai-ink); }
.monitor-identity { min-width:0; text-align:left; }
.monitor-identity strong { display:block; font-size:.95rem; font-weight:650; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.monitor-identity small { display:block; color:var(--ai-muted); font-size:.78rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.monitor-runtime { text-align:right; font-size:.76rem; line-height:1.8; }
.monitor-runtime small { display:block; color:var(--ai-muted); font-size:.75rem; }
.monitor-caret { color:var(--ai-muted); font-size:1.3rem; text-align:right; }
.monitor-pill { display:inline-block; width:fit-content; border-radius:20px; padding:.15rem .55rem;
  font-size:.66rem; font-weight:700; letter-spacing:.035em; background:#edf2f9; color:#48617f; }
.monitor-pill--active, .monitor-pill--success { background:#e6f5ee; color:#147b54; }
.monitor-pill--paused { background:#fff2d9; color:#8b5b08; }
.monitor-pill--archived { background:#e5e9ef; color:#617083; }
.monitor-pill--failed, .monitor-pill--error { background:#ffebee; color:#b8233c; }
.monitor-pill--running { background:#eaf2ff; color:#0764e8; }
.monitor-kpis { display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:1rem; padding:.6rem 0 1rem; }
.monitor-kpis small { display:block; color:var(--ai-muted); font-size:.74rem; margin-bottom:.35rem; }
.monitor-kpis strong { display:block; font-size:.95rem; color:var(--ai-ink); overflow-wrap:anywhere; }
.monitor-fields { display:grid; grid-template-columns:minmax(8rem,1fr) minmax(0,2fr); gap:.55rem 1rem;
  font-size:.83rem; margin:.4rem 0 1rem; }
.monitor-fields dt { color:var(--ai-muted); font-weight:400; }
.monitor-fields dd { color:var(--ai-ink); margin:0; overflow-wrap:anywhere; }
@media(max-width:900px) {
  .monitor-row { grid-template-columns:5.5rem minmax(0,1fr) 1rem; gap:.5rem; padding:.45rem 0; }
  .monitor-runtime { grid-column:2; grid-row:2; text-align:left; }
  .monitor-runtime small { display:inline; margin-left:.5rem; }
  .monitor-caret { grid-column:3; grid-row:1 / 3; }
  .monitor-kpis { grid-template-columns:repeat(2,minmax(0,1fr)); }
}
</style>
"""


def pill(label):
    tone = label.lower() if label in {'ACTIVE', 'PAUSED', 'ARCHIVED', 'SUCCESS', 'FAILED', 'ERROR', 'RUNNING'} else 'neutral'
    return f'<span class="monitor-pill monitor-pill--{tone}">{e(label)}</span>'


def interval(seconds):
    return f'{seconds // 60} min' if seconds % 60 == 0 else f'{seconds} s'


def monitor_row(monitor, run, expanded, redact):
    d = monitor.definition
    state = 'ARCHIVED' if monitor.archived else 'ACTIVE' if monitor.enabled else 'PAUSED'
    next_run = monitor.next_run_at.strftime('%d %b %H:%M UTC') if monitor.enabled and not monitor.archived else 'Not scheduled'
    classes = 'monitor-row' + (' monitor-row--expanded' if expanded else '') + (' monitor-row--archived' if monitor.archived else '')
    name, scope = e(redact(d.name)), e(redact(d.namespace + ' / ' + d.workload))
    return (f'<div class="{classes}">{pill(state)}'
            f'<div class="monitor-identity"><strong title="{name}">{name}</strong><small title="{scope}">{scope}</small></div>'
            f'<div class="monitor-runtime">Last run: {pill(run.status.value) if run else "No runs"}'
            f'<small>{e(interval(d.interval_seconds))} · Next: {e(next_run)}</small></div>'
            f'<span class="monitor-caret" aria-hidden="true">{"⌄" if expanded else "›"}</span></div>')


def fields(rows, redact):
    return '<dl class="monitor-fields">' + ''.join(
        f'<dt>{e(label)}</dt><dd>{e(redact(str(value)))}</dd>' for label, value in rows) + '</dl>'


def kpis(monitor, latest):
    def stamp(value):
        return value.strftime('%d %b %H:%M UTC') if value else 'None'
    rows = [('Last Run', latest.status.value if latest else 'No runs'),
            ('Last Successful End', stamp(monitor.last_successful_end)),
            ('Next Run', stamp(monitor.next_run_at) if monitor.enabled and not monitor.archived else 'Not scheduled'),
            ('Interval', interval(monitor.definition.interval_seconds)),
            ('Records / Events', f'{latest.counts.events_retrieved} / {latest.counts.logical_events}' if latest else '—')]
    return '<div class="monitor-kpis">' + ''.join(
        f'<div><small>{e(label)}</small><strong>{e(value)}</strong></div>' for label, value in rows) + '</div>'
