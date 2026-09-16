from __future__ import annotations
import csv, io, json, os, sys, tempfile
from datetime import datetime
from html import escape
from pathlib import Path
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / 'src' / 'backend'
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
from full_pipeline_v2 import FullAIOpsPipelineV2
import theme as ui

st.set_page_config(page_title='AOP • Açıklanabilir SRE Zekâsı', page_icon='◈', layout='wide')
st.markdown(ui.CSS, unsafe_allow_html=True)

@st.cache_resource
def get_pipeline():
    # Keşif: DeepSeek. Olay/RCA yorumu: Qwen. Qwen başarısız olsa bile deterministik RCA korunur.
    return FullAIOpsPipelineV2(use_ai_rca=True)

ALARM_SEVERITY = {
    1: "INFO",
    2: "WARNING",
    3: "ERROR",
    4: "ERROR",
    5: "CRITICAL",
}

def _normalize_alarm_record(record):
    """Normalize hackathon alarm rows without changing generic log semantics."""
    row = dict(record or {})
    if row.get("alarm_id") and row.get("alarm_type"):
        try:
            level = int(row.get("severity"))
            row["severity"] = ALARM_SEVERITY.get(level, row.get("severity"))
            row["source_severity"] = level
        except (TypeError, ValueError):
            pass
        # JsonParser promotes service/host from attributes into CanonicalEvent.resource.
        if row.get("servis") and not row.get("service"):
            row["service"] = row.get("servis")
    return row

def _uploaded_to_pipeline_text(upload):
    """Convert CSV/JSON alarm packages to one-JSON-object-per-line input.

    TXT/MD/LOG remain plain text unless the whole TXT content is valid JSON.
    This keeps the existing segmentation/parser pipeline unchanged while making
    the hackathon file formats directly uploadable.
    """
    raw = upload.getvalue()
    text = raw.decode("utf-8-sig", errors="replace")
    suffix = Path(upload.name).suffix.lower()

    if suffix == ".csv":
        rows = list(csv.DictReader(io.StringIO(text)))
        if not rows:
            raise ValueError("CSV dosyasında veri satırı bulunamadı.")
        return "\n".join(
            json.dumps(_normalize_alarm_record(row), ensure_ascii=False)
            for row in rows
        ) + "\n", ".log"

    if suffix in {".json", ".txt"}:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            data = None
        if isinstance(data, list) and all(isinstance(x, dict) for x in data):
            return "\n".join(
                json.dumps(_normalize_alarm_record(row), ensure_ascii=False)
                for row in data
            ) + "\n", ".log"
        if isinstance(data, dict):
            return json.dumps(_normalize_alarm_record(data), ensure_ascii=False) + "\n", ".log"

    return text, suffix or ".log"

def run_uploaded(upload):
    suffix = Path(upload.name).suffix.lower()
    if suffix == '.zip':
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as f:
            f.write(upload.getvalue()); path=f.name
        try:
            return get_pipeline().process_package(path)
        finally:
            try: os.unlink(path)
            except OSError: pass
    content, suffix = _uploaded_to_pipeline_text(upload)
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode='w', encoding='utf-8', newline='') as f:
        f.write(content); path=f.name
    try: return get_pipeline().process_file(path)
    finally:
        try: os.unlink(path)
        except OSError: pass

def smap(signals):
    return {s.get('signal_id'): s for s in signals}

def _fmt_ms(v):
    try: return datetime.fromtimestamp(int(v)/1000).strftime('%H:%M:%S')
    except Exception: return '—'

def html(markup):
    st.markdown(markup, unsafe_allow_html=True)

def esc(value):
    """Log/LLM kaynaklı metni HTML kartlarına gömmeden önce kaçışla."""
    return escape(str(value), quote=True)

AKSIYONLAR = {
    'collect_more_evidence': 'Daha fazla kanıt topla',
    'prepare_operator_review': 'Operatör incelemesine hazırla',
    'increase_observation_and_enrich_context': 'Gözlemi artır ve bağlamı zenginleştir',
}

# ---------------------------------------------------------------- Kenar çubuğu
with st.sidebar:
    html(ui.brand_block())
    st.caption('Ajan Tabanlı Operasyon Platformu')

    html(ui.side_label('Görünüm'))
    page = st.selectbox('Görünüm', ['Operasyon Analizi', 'Katman İzleme'], key='view', label_visibility='collapsed')

    html(ui.side_label('Veri Paketi'))
    upload = st.file_uploader(
        'Hackathon veri paketini seç', type=['zip','csv','json','txt','md','log'],
        label_visibility='collapsed',
        help='Önerilen: alarms.json + host_inventory.csv + service_dependencies.csv içeren tek ZIP. Tekil dosyalar geriye dönük desteklenir.')
    st.caption('Önerilen: ZIP paket • Ayrıca CSV / JSON / TXT / MD / LOG')
    run = st.button('◈  Analizi Başlat', type='primary', width='stretch')

    html(ui.side_label('Model Yönlendirmesi'))
    html(
        ui.pill('Keşif · DeepSeek', 'info')
        + ui.pill('RCA yorumu · Qwen', 'yellow')
        + ui.pill('Karar motoru · Deterministik', 'good')
    )
    st.caption('Korelasyon, nitelik kapısı ve olay üretimi deterministiktir; LLM bu kararların yerine geçmez.')

# ----------------------------------------------------------------------- Hero
html(ui.hero(
    'Turkcell • Açıklanabilir SRE Zekâsı',
    'Ajan Tabanlı <em>Operasyon Platformu</em>',
    'Ham loglardan şablonlara, sinyal adaylarına, nitelikli sinyallere, korelasyonlara ve açıklanabilir olay hipotezlerine — her adımın kanıtı görünür.',
    [('9 katmanlı pipeline', 'yellow'), ('XAI karar izi', 'info'), ('İnsan onaylı aksiyon', 'good')],
))

if run and upload:
    try:
        with st.spinner('Loglar analiz ediliyor...'):
            st.session_state['result'] = run_uploaded(upload)
    except Exception as e:
        st.exception(e)

result = st.session_state.get('result')
if not result:
    html(ui.empty_state(
        'Analiz bekleniyor',
        'Üç ana dosyayı içeren ZIP paketini soldan yükleyip “Analizi Başlat” düğmesine basın. '
        'Paket içindeki inventory ve service dependency verileri korelasyon/RCA bağlamında kullanılır.'))
    st.stop()

stats=result.get('stats',{}); signals=result.get('signals',[]); qualified=result.get('qualified_signals',[]); correlations=result.get('correlations',[]); incidents=result.get('incidents',[]); rca=result.get('rca',[]); plans=result.get('plans',[]); case_analysis=result.get('case_analysis') or {}; case_error=result.get('case_analysis_error'); signal_by_id=smap(signals)
unique_templates=len({s.get('template_id') for s in signals if s.get('template_id')})

# --------------------------------------------------------------- Veri paketi
package_summary=result.get('package_summary') or {}
if package_summary:
    html(ui.section('Veri Paketi', 'Yüklenen paketin kapsamı ve alanlar arası tutarlılık kontrolü.'))
    html(ui.kpi_grid([
        ui.kpi('Alarm', package_summary.get('alarm_count',0), 'Paketteki toplam alarm kaydı'),
        ui.kpi('Host', package_summary.get('host_count',0), 'Inventory’deki sunucu sayısı', 'info'),
        ui.kpi('Servis Bağımlılığı', package_summary.get('dependency_count',0), 'Topoloji kenar sayısı', 'info'),
    ]))
    dq=package_summary.get('data_quality',{})
    if any(dq.values()): st.warning(f'Paket veri kalite uyarıları: {dq}')
    else: html('<div style="margin-top:.7rem">' + ui.pill('Alarm ↔ host inventory servis/DC/rack eşleşmeleri tutarlı', 'good') + '</div>')

# ============================================================ KATMAN İZLEME
if page == 'Katman İzleme':
    html(ui.section(
        'Uçtan Uca Katman İzleme',
        'Aynı logun her katmanda nasıl dönüştüğünü Girdi → Uygulanan Karar → Çıktı akışıyla inceleyin. '
        'İlk üç katmanda performans için en fazla 200 örnek kayıt gösterilir; sayaçlar tüm dosyayı temsil eder.'))
    trace = result.get('pipeline_trace', {})
    layer_options = [
        '1 • Segmentasyon', '2 • Ayrıştırma', '3 • Şablonlama', '4 • Sinyal Adayı',
        '5 • Nitelik Kapısı', '6 • Korelasyon', '7 • Olay Üretimi', '8 • RCA / Case Analizi', '9 • Planlama'
    ]
    layer = st.selectbox('İncelenecek katman', layer_options, key='layer')

    seg=trace.get('segmentation',{}); par=trace.get('parser',{}); tpl=trace.get('template',{})
    layer_info = {
        '1 • Segmentasyon': {
            'input': (stats.get('raw_lines', stats.get('segmented',0)), 'ham log kaydı', 'Dosyadan gelen ham log akışı'),
            'decision': ('Olay sınırı', 'Multiline ve başlangıç desenleri', 'DeepSeek yalnız yeni format keşfinde sınır kuralına yardımcı olur; doğrulanan kural deterministik uygulanır.'),
            'output': (seg.get('count', stats.get('segmented',0)), 'log olayı', 'Çok satırlı kayıtlar tek olay halinde birleştirilir.'),
        },
        '2 • Ayrıştırma': {
            'input': (seg.get('count', stats.get('segmented',0)), 'segment edilmiş olay', 'Segmentasyon katmanının olayları'),
            'decision': ('CanonicalEvent', 'Format tespiti + alan normalizasyonu', 'Timestamp, severity, message ve kaynak alanları ortak şemaya dönüştürülür.'),
            'output': (par.get('count', stats.get('parsed',0)), 'canonical olay', 'Downstream katmanları artık log formatından bağımsız çalışır.'),
        },
        '3 • Şablonlama': {
            'input': (par.get('count', stats.get('parsed',0)), 'canonical olay', 'Normalize edilmiş olay mesajları'),
            'decision': ('Template çıkarımı', 'Değişken değerleri genelle', 'ID, sayı ve değişken parçalar ayrıştırılarak tekrar eden log davranışı bulunur.'),
            'output': (unique_templates, 'benzersiz şablon', f'{tpl.get("count", stats.get("templated",0))} olay şablonlandı.'),
        },
        '4 • Sinyal Adayı': {
            'input': (unique_templates, 'benzersiz şablon', 'Şablonlanmış olay kümeleri'),
            'decision': ('Pencereleme', 'Template + bağlam + zaman', 'Benzer olaylar zaman penceresinde gruplanır; henüz incident kararı verilmez.'),
            'output': (len(signals), 'sinyal adayı', 'Qualification katmanına gönderilecek aday kümeler.'),
        },
        '5 • Nitelik Kapısı': {
            'input': (len(signals), 'sinyal adayı', 'Template katmanından türetilmiş adaylar'),
            'decision': ('Deterministik gate', 'Şiddet + tekrar + burst + kanıt', 'Yeterli operasyonel kanıtı olmayan adaylar gürültü olarak bastırılır.'),
            'output': (len(qualified), 'nitelikli sinyal', f'{stats.get("noise_suppressed",0)} aday gürültü olarak bastırıldı.'),
        },
        '6 • Korelasyon': {
            'input': (len(qualified), 'nitelikli sinyal', 'Nitelik kapısını geçen sinyaller'),
            'decision': ('Kanıt bağı', 'Topoloji + zaman + alarm semantiği', 'Service dependency yönü, host inventory, DC/rack ve semptom zinciri birlikte değerlendirilir; zaman tek başına yeterli değildir.'),
            'output': (len(correlations), 'korelasyon bağı', 'Birbirini destekleyen sinyaller arasında açıklanabilir bağlar.'),
        },
        '7 • Olay Üretimi': {
            'input': (len(qualified), 'nitelikli sinyal', f'{len(correlations)} korelasyon bağı ile birlikte değerlendirilir.'),
            'decision': ('Incident adayı', 'Korelasyon grubu veya güçlü tekil hata', 'Her sinyal incident yapılmaz; yeterli kanıt taşıyan gruplar olay adayına dönüşür.'),
            'output': (len(incidents), 'olay adayı', 'RCA için anlamlı ve sınırlı olay seti.'),
        },
        '8 • RCA / Case Analizi': {
            'input': (len(incidents), 'olay adayı', f'{len(qualified)} nitelikli sinyal ve {len(correlations)} bağ kanıt olarak kullanılır.'),
            'decision': ('RCA + Qwen', 'Deterministik hipotez → tek uzman case yorumu', 'Deterministik RCA korunur; Qwen mevcut kanıtı yorumlar, yeni topoloji veya kanıt uydurmamalıdır.'),
            'output': (len(rca), 'RCA hipotezi', 'Qwen case analizi mevcut.' if case_analysis else 'Qwen çıktısı yok; deterministik RCA korunuyor.'),
        },
        '9 • Planlama': {
            'input': (len(rca), 'RCA hipotezi', 'Olay ve RCA kanıtları'),
            'decision': ('Sonraki adım', 'Güvenli inceleme önerisi', 'Sistem yalnız operatöre önerilecek sonraki inceleme adımını üretir.'),
            'output': (len(plans), 'inceleme planı', 'Operatörün değerlendirebileceği açıklanabilir sonraki adımlar.'),
        },
    }
    info=layer_info[layer]
    c1,arrow1,c2,arrow2,c3=st.columns([1,.10,1,.10,1])
    with c1: html(ui.trace_card('GİRDİ', *info['input']))
    with arrow1: html(ui.trace_arrow())
    with c2: html(ui.trace_card('UYGULANAN KARAR', *info['decision'], kind='decision'))
    with arrow2: html(ui.trace_arrow())
    with c3: html(ui.trace_card('ÇIKTI', *info['output']))

    html(ui.section('Katman Kanıtı', 'Aşağıdaki kayıtlar yukarıdaki kararın gerçek pipeline çıktısındaki karşılığını gösterir.'))
    if layer.startswith('1'):
        st.dataframe(seg.get('items',[]), width='stretch', hide_index=True)
    elif layer.startswith('2'):
        st.dataframe(par.get('items',[]), width='stretch', hide_index=True)
    elif layer.startswith('3'):
        st.dataframe(tpl.get('items',[]), width='stretch', hide_index=True)
    elif layer.startswith('4'):
        st.dataframe(signals, width='stretch', hide_index=True)
    elif layer.startswith('5'):
        qrows=[{'Sinyal':s.get('signal_id'),'Şablon':s.get('template',''),'Adet':s.get('count',0),'Önem':s.get('severity_min'),'Puan':s.get('qualification_score',0),'Karar':'GEÇTİ' if s.get('qualified') else 'BASTIRILDI','Kanıt':', '.join(s.get('qualification_evidence',[]))} for s in signals]
        st.dataframe(qrows, width='stretch', hide_index=True)
    elif layer.startswith('6'):
        if correlations: st.dataframe(correlations, width='stretch', hide_index=True)
        else: html(ui.empty_state('Korelasyon bağı yok', 'Bu logda sinyaller arasında kanıtlanabilir bir bağ oluşmadı.'))
    elif layer.startswith('7'):
        if incidents: st.dataframe(incidents, width='stretch', hide_index=True)
        else: html(ui.empty_state('Olay adayı yok', 'Bu logda olay eşiğine ulaşan kanıt kümesi oluşmadı.'))
    elif layer.startswith('8'):
        if case_analysis:
            html(ui.section('Qwen Uzman Case Analizi'))
            x1,x2=st.columns([1.4,.6])
            with x1:
                html('<div class="card card--accent"><div class="card__title">Uzman Yorumu</div>'
                     f'<div class="card__row"><b>Durum özeti:</b> {esc(case_analysis.get("durum_ozeti","—"))}</div>'
                     f'<div class="card__row"><b>Kök neden hipotezi:</b> {esc(case_analysis.get("kok_neden_hipotezi","—"))}</div></div>')
            with x2:
                st.metric('Uzman güveni', case_analysis.get('guven','—'))
                html('<div class="card"><div class="card__title">Nedensellik</div>'
                     f'<div class="card__row">{esc(case_analysis.get("nedensellik_durumu","—"))}</div></div>')
        elif case_error: st.warning(case_error)
        html(ui.section('Deterministik RCA Kanıtı'))
        st.json(rca, expanded=False)
    else:
        if plans: st.dataframe(plans, width='stretch', hide_index=True)
        else: html(ui.empty_state('Plan yok', 'Bu log için inceleme planı üretilmedi.'))

    html(ui.section('Uçtan Uca Akış', '✓ tamamlanan önceki katmanları, ● şu an incelenen katmanı, ○ sonraki katmanları gösterir.'))
    flow_steps=[('Segmentasyon',stats.get('segmented',0),'olay'),('Ayrıştırma',stats.get('parsed',0),'olay'),('Şablonlama',stats.get('templated',0),'olay'),('Sinyal Adayı',len(signals),'aday'),('Nitelik',len(qualified),'sinyal'),('Korelasyon',len(correlations),'bağ'),('Olay',len(incidents),'aday'),('RCA',len(rca),'hipotez'),('Plan',len(plans),'plan')]
    html(ui.flow(flow_steps, active_index=layer_options.index(layer)))
    st.stop()

# ========================================================= OPERASYON ANALİZİ
html(ui.section('Operasyon Özeti', 'Gürültüden kanıta: pipeline’ın bu çalıştırmada ürettiği ana büyüklükler.'))
noise=stats.get('noise_suppressed',0)
html(ui.kpi_grid([
    ui.kpi('Log Olayı', stats.get('templated',0), 'Şablonlanmış toplam olay'),
    ui.kpi('Benzersiz Şablon', unique_templates, 'Tekrar eden log davranışı', 'info'),
    ui.kpi('Sinyal Adayı', len(signals), 'Pencerelenmiş aday kümeler', 'info'),
    ui.kpi('Nitelikli Sinyal', len(qualified), 'Deterministik kapıyı geçti', 'good'),
    ui.kpi('Bastırılan Gürültü', noise, 'Operasyonel kanıtı yetersiz', 'muted'),
    ui.kpi('Korelasyon Bağı', len(correlations), 'Açıklanabilir sinyal bağı', 'warn'),
    ui.kpi('Olay Adayı', len(incidents), 'RCA’ya giden olay seti', 'bad' if incidents else 'muted'),
]))

html(ui.section('İşlem Hattı', '✓ katmanın hatasız çalıştığını; sayı ise o katmanın kendi çıktı türünü gösterir. Farklı katmanlardaki sayılar aynı nesne türü değildir.'))
html(ui.flow([
    ('Segmentasyon',stats.get('segmented',0),'olay'),('Ayrıştırma',stats.get('parsed',0),'olay'),
    ('Şablonlama',stats.get('templated',0),'olay'),('Sinyal Adayı',len(signals),'aday'),
    ('Nitelik Kapısı',len(qualified),'sinyal'),('Korelasyon',len(correlations),'bağ'),
    ('Olay Üretimi',len(incidents),'aday'),('RCA',len(rca),'hipotez'),
]))

left,right=st.columns([1.28,.72])
with left:
    html(ui.section('Sinyal Analizi', 'En yüksek puanlı 100 sinyal adayı.'))
    html(ui.kpi_grid([
        ui.kpi('Nitelik Oranı', f'{100*len(qualified)/max(1,len(signals)):.1f}%', 'Kapıyı geçen aday oranı', 'good'),
        ui.kpi('Gürültü Azaltma', f'{100*noise/max(1,len(signals)):.1f}%', 'Bastırılan aday oranı', 'muted'),
        ui.kpi('Bilinen Kapsam', sum(1 for s in signals if s.get('scope') not in ('genel','unknown/unknown/unknown')), 'Servis/host bağlamı çözülmüş', 'info'),
    ]))
    st.write('')
    rows=[{'Şablon':s.get('template',''),'Adet':s.get('count',0),'Önem':s.get('severity_min'),'Puan':s.get('qualification_score',0),'Nitelikli':s.get('qualified',False),'Kapsam':s.get('scope','genel')} for s in sorted(signals,key=lambda x:(x.get('qualified',False),x.get('qualification_score',0),x.get('count',0)),reverse=True)[:100]]
    st.dataframe(rows,width='stretch',hide_index=True,height=360)
with right:
    html(ui.section('Karar Açıklaması', 'En güçlü kanıt taşıyan sinyal ve korelasyon durumu.'))
    if qualified:
        top=max(qualified,key=lambda x:(float(x.get('qualification_score',0)),int(x.get('count',0))))
        html('<div class="card card--accent"><div class="card__title">Örnek Nitelikli Sinyal</div>'
             f'<div class="card__mono">{esc(top.get("template",""))}</div>'
             f'<div class="card__row"><b>Puan:</b> {esc(top.get("qualification_score",0))} &nbsp;·&nbsp; '
             f'<b>Adet:</b> {esc(top.get("count",0))} &nbsp;·&nbsp; <b>Kapsam:</b> {esc(top.get("scope","genel"))}</div>'
             f'<div style="margin-top:.6rem">{ui.chips(top.get("qualification_evidence",[]))}</div></div>')
    else:
        html(ui.empty_state('Nitelikli sinyal yok', 'Hiçbir aday deterministik nitelik kapısını geçemedi.'))
    st.write('')
    if correlations:
        html('<div class="card"><div class="card__title">Korelasyon Durumu</div>'
             f'<div class="card__row">{ui.pill(f"{len(correlations)} korelasyon bağı bulundu", "good")}</div></div>')
    else:
        html('<div class="card"><div class="card__title">Korelasyon Durumu</div>'
             f'<div class="card__row">{ui.pill("Korelasyon bağı bulunmadı", "muted")}</div>'
             '<div class="card__row" style="font-size:.84rem;color:var(--ink-3)">Zaman yakınlığı tek başına korelasyon sayılmaz; '
             'servis, bileşen, sunucu, bağımlılık veya ortak hata ailesi kanıtı gerekir.</div></div>')

st.divider()
html(ui.section('Olay ve XAI İncelemesi', 'Her olay için kök neden hipotezi, kanıt zinciri ve operatör aksiyonu.'))

if not incidents:
    html(ui.empty_state(
        'Olay adayı oluşmadı',
        'Nitelikli sinyal bulunabilir; ancak korelasyon veya güçlü tekil hata kanıtı olay eşiğine ulaşmamış olabilir.'))
else:
    imap={i['incident_id']:i for i in incidents}; rmap={x['incident_id']:x for x in rca}; pmap={x['incident_id']:x for x in plans}
    incident_rows=[]
    for inc0 in incidents:
        plan0=pmap.get(inc0['incident_id'],{}); root0=inc0.get('probable_root') or {}
        incident_rows.append({
            'Olay':inc0['incident_id'], 'Ana Sorun':root0.get('entity','—'), 'Kök Alarm':root0.get('alarm_type','—'),
            'Etkilenen Servisler':', '.join(inc0.get('services',[])), 'Alarm Sayısı':inc0.get('event_count',0),
            'Başlangıç':_fmt_ms(inc0.get('start_ms')), 'Bitiş':_fmt_ms(inc0.get('end_ms')), 'Güven':inc0.get('confidence',0),
            'İlk Aksiyon':AKSIYONLAR.get(plan0.get('recommended_next_step'),plan0.get('recommended_next_step','—')),
            'Sahip':plan0.get('owner','SRE / NOC'), 'Durum':plan0.get('status','Açık')})
    st.dataframe(incident_rows,width='stretch',hide_index=True)

    selected=st.selectbox('İncelenecek olay',list(imap),key='incident',format_func=lambda x:f"{x} • {((imap[x].get('probable_root') or {}).get('entity','—'))} • {imap[x].get('event_count',0)} alarm")
    inc=imap[selected]; rr=rmap.get(selected,{}); plan=pmap.get(selected,{}); candidates=rr.get('root_cause_candidates',[])
    proot=inc.get('probable_root') or {}
    conf=float(inc.get('confidence',0) or 0)

    head_l,head_r=st.columns([1.35,.65])
    with head_l:
        html('<div class="card card--accent"><div class="card__title">Olası Ana Sorun</div>'
             f'<div class="card__mono">{esc(proot.get("entity","—"))}</div>'
             f'<div class="card__row">{ui.pill(str(proot.get("alarm_type","—")), "bad")}'
             f'{ui.pill("tür: " + str(proot.get("kind","—")), "info")}'
             f'{ui.pill(_fmt_ms(inc.get("start_ms")) + " → " + _fmt_ms(inc.get("end_ms")))}</div>'
             f'{ui.meter(conf, f"Olay güveni: {conf:.2f}")}</div>')
    with head_r:
        html(ui.kpi_grid([
            ui.kpi('Sinyal', inc.get('signal_count',0), tone='info'),
            ui.kpi('Log Olayı', inc.get('event_count',0), tone='info'),
            ui.kpi('Korelasyonlu', 'Evet' if inc.get('correlated') else 'Hayır', tone='good' if inc.get('correlated') else 'muted'),
        ]))

    st.write('')
    if case_analysis:
        html(ui.pill(f"Uzman case analizi aktif · Qwen {case_analysis.get('analysis_model','')}", 'good'))
    elif case_error:
        html(ui.pill('Qwen uzman analizi alınamadı — deterministik RCA korunuyor', 'warn'))
    else:
        html(ui.pill('Uzman case analizi yok — deterministik RCA gösteriliyor', 'warn'))
    st.write('')

    tabs=st.tabs(['Uzman Case Analizi','Kök Neden Hipotezi','Korelasyon Kanıtı','Sinyal Kanıtları','Önerilen Aksiyon'])
    with tabs[0]:
        if case_analysis:
            html('<div class="card card--accent">'
                 f'<div class="card__row"><b>Durum özeti:</b> {esc(case_analysis.get("durum_ozeti","—"))}</div>'
                 f'<div class="card__row"><b>Kök neden hipotezi:</b> {esc(case_analysis.get("kok_neden_hipotezi","—"))}</div>'
                 f'<div class="card__row"><b>Uzman güveni:</b> {esc(case_analysis.get("guven","—"))} &nbsp;·&nbsp; '
                 f'<b>Nedensellik:</b> {esc(case_analysis.get("nedensellik_durumu","—"))}</div></div>')
            st.write('')
            x1,x2=st.columns(2)
            with x1:
                st.markdown('**Karar gerekçesi**')
                for item in case_analysis.get('karar_gerekcesi',[]): st.markdown(f'- {item}')
                st.markdown('**Eksik kanıtlar**')
                for item in case_analysis.get('eksik_kanitlar',[]): st.markdown(f'- {item}')
            with x2:
                st.markdown('**Alternatif hipotezler**')
                for item in case_analysis.get('alternatif_hipotezler',[]): st.markdown(f'- {item}')
                st.markdown('**Önerilen incelemeler**')
                for item in case_analysis.get('onerilen_incelemeler',[]): st.markdown(f'- {item}')
        else:
            html(ui.empty_state('Qwen çıktısı yok', 'Deterministik kanıtlar diğer sekmelerde gösteriliyor.'))
    with tabs[1]:
        if candidates:
            for n,cand in enumerate(candidates,1):
                s=signal_by_id.get(cand.get('signal_id'),{})
                score=float(cand.get('score',0) or 0)
                html('<div class="card" style="margin-bottom:.6rem">'
                     f'<div class="card__title">Hipotez {n} · puan {score:.2f}</div>'
                     f'<div class="card__mono">{esc(s.get("template",cand.get("signal_id")))}</div>'
                     f'{ui.chips(cand.get("evidence",[]))}'
                     f'{ui.meter(min(score,1.0))}</div>')
        else:
            html(ui.empty_state('Hipotez üretilemedi', 'Kök neden sıralaması için yeterli kanıt yok.'))
        st.caption('Deterministik RCA doğrulanmış neden değil, eldeki kanıtlara göre sıralanmış hipotezdir.')
    with tabs[2]:
        ids=set(inc.get('signal_ids',[])); local=[e for e in correlations if e.get('source') in ids and e.get('target') in ids]
        if local:
            corr_rows=[{'Kaynak':signal_by_id.get(e['source'],{}).get('template',e['source']),'Hedef':signal_by_id.get(e['target'],{}).get('template',e['target']),'Puan':e.get('score'),'Zaman Farkı (ms)':e.get('time_gap_ms'),'Kanıt':', '.join(e.get('evidence',[]))} for e in local]
            st.dataframe(corr_rows,width='stretch',hide_index=True)
        else:
            html(ui.empty_state('Korelasyon bağı yok', 'Bu olay güçlü tekil hata olarak oluştu.'))
    with tabs[3]:
        signal_rows=[{'Şablon':signal_by_id.get(sid,{}).get('template',sid),'Adet':signal_by_id.get(sid,{}).get('count'),'Önem':signal_by_id.get(sid,{}).get('severity_min'),'Puan':signal_by_id.get(sid,{}).get('qualification_score'),'Kapsam':signal_by_id.get(sid,{}).get('scope'),'Kanıt':', '.join(signal_by_id.get(sid,{}).get('qualification_evidence',[]))} for sid in inc.get('signal_ids',[])]
        st.dataframe(signal_rows,width='stretch',hide_index=True)
        with st.expander('Olay bağlamı'):
            st.json(inc.get('context',{}))
    with tabs[4]:
        needs_approval = plan.get('requires_human_approval', True)
        html('<div class="card card--accent"><div class="card__title">Operatör Aksiyonu</div>'
             f'<div class="card__row"><b>Önerilen ilk aksiyon:</b> {esc(AKSIYONLAR.get(plan.get("recommended_next_step"), plan.get("recommended_next_step","Plan yok")))}</div>'
             f'<div class="card__row"><b>Sahip:</b> {esc(plan.get("owner","SRE / NOC"))} &nbsp;·&nbsp; '
             f'<b>Durum:</b> {esc(plan.get("status","Açık"))} &nbsp;·&nbsp; <b>Güven:</b> {esc(plan.get("confidence","—"))}</div>'
             f'<div style="margin-top:.6rem">{ui.pill("İnsan onayı gerekli", "warn") if needs_approval else ui.pill("İnsan onayı gerekli değil", "good")}</div></div>')

st.divider()
html(ui.section('Teknik Ayrıntılar', 'Ham pipeline çıktıları — denetim ve hata ayıklama için.'))
with st.expander('Tüm sinyal adayları'):
    st.dataframe(signals,width='stretch',hide_index=True)
with st.expander('Korelasyonlar'):
    if correlations:
        st.dataframe(correlations,width='stretch',hide_index=True)
    else:
        st.caption('Korelasyon yok.')
with st.expander('Uzman case analizi ham çıktısı'):
    st.json(case_analysis if case_analysis else {'durum': 'Qwen çıktısı yok', 'hata': case_error}, expanded=False)
with st.expander('Deterministik RCA ham çıktısı'):
    st.json(rca,expanded=False)
with st.expander('Tüm sonuç JSON'):
    st.json(result,expanded=False)
