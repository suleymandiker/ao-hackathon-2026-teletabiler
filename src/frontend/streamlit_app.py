"""AI-IN-AI investigation navigation and synchronous analysis flow."""
from __future__ import annotations

import math
import os
from pathlib import Path
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
for directory in (ROOT / 'src' / 'backend', Path(__file__).resolve().parent):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

import opensearch_application as openshift
from analysis_runtime import get_analysis_lock, get_pipeline, run_uploaded
from presentation import InvestigationPresenter
import investigation_views as views
import theme


LOGO = Path(__file__).resolve().parent / 'assets' / 'ai-in-ai-logo.png'
OPENSHIFT = 'OpenShift logları'
FILES = 'Dosya / paket'
st.set_page_config(page_title='AI-IN-AI Operations', page_icon='🔎', layout='wide')
views.html(theme.CSS)


def begin(source):
    st.session_state['navigation'] = 'Investigations'
    st.session_state['active_source'] = source
    st.session_state['analysis_source'] = source
    st.session_state['new_investigation'] = True


def resume():
    st.session_state['navigation'] = 'Investigations'
    st.session_state['active_source'] = st.session_state['result_source']
    st.session_state['analysis_source'] = st.session_state['result_source']
    st.session_state['new_investigation'] = False


def show_form():
    st.session_state['new_investigation'] = True


def select_source():
    st.session_state['active_source'] = st.session_state['analysis_source']
    show_form()


try:
    connection = openshift.load_connection()
except openshift.ApplicationError:
    connection = None
redact = lambda value: openshift.safe_text(value, connection)

with st.sidebar:
    st.image(str(LOGO), width=161)
    st.caption('Operations')
    st.divider()
    page = st.radio('Gezinme', ['Overview', 'Investigations', 'Log Patterns', 'Incidents'],
                    key='navigation', label_visibility='collapsed')
    st.divider()
    st.caption('Tek oturum · Sonlu analiz')
    st.caption('Kararlar ve kanıtlar birlikte.')

# Page ownership must survive Streamlit's cleanup of hidden form widgets.
source = st.session_state.get('active_source', OPENSHIFT)
view = st.session_state.get('result') if st.session_state.get('result_source') == source else None
views.html(theme.header(page, 'OpenShift yapılandırıldı' if connection else 'OpenShift yapılandırılmadı'))


def overview():
    st.title('AI-IN-AI Operations')
    views.html(theme.intro('OpenShift loglarını analiz edin; önemli olayları, ilişkileri ve olası kök nedenleri keşfedin.'))
    left, right = st.columns(2, gap='medium')
    with left, st.container(border=True):
        views.html(theme.task('◎', 'OpenShift Investigation', 'OpenShift loglarını uçtan uca analiz ederek olayları, sinyalleri ve olası kök nedenleri bulun.'))
        st.button('Analizi Başlat →', key='start_openshift', type='primary', width='stretch', on_click=begin, args=(OPENSHIFT,))
    with right, st.container(border=True):
        views.html(theme.task('▤', 'Dosya / Paket Analizi', 'Log, JSON, CSV veya yapılandırılmış alarm paketini analiz edin.'))
        st.button('Dosya Seç →', key='start_file', width='stretch', on_click=begin, args=(FILES,))
    current = st.session_state.get('result')
    if current:
        st.subheader('Bu oturumdaki son analiz')
        with st.container(border=True):
            st.text(current.source + (' · ' + current.target if current.target else ''))
            st.text(current.title)
            st.button('Investigation sonucunu aç', key='resume', on_click=resume)
    else:
        views.html(theme.empty('Bir investigation ile başlayın', 'Sonuç, açıklanabilir pipeline ve kanıtlar analiz tamamlandığında burada hazır olur.'))


def new_investigation():
    st.title('Yeni Investigation')
    views.html(theme.intro('Analiz edilecek kaynağı ve kapsamı seçin.'))
    if 'analysis_source' not in st.session_state:
        st.session_state['analysis_source'] = source
    selected_source = st.radio('Kaynak', [OPENSHIFT, FILES], key='analysis_source', horizontal=True, on_change=select_source)
    upload = request = None
    target = ''
    form, guide = st.columns([1.7, 1], gap='large')
    with form, st.container(border=True):
        if selected_source == OPENSHIFT:
            if connection is None:
                st.warning('OpenShift bağlantı ayarları hazır değil. Uygulama yöneticinizle iletişime geçin.')
            namespace = st.text_input('Namespace', value=os.environ.get('OPENSEARCH_SMOKE_NAMESPACE', ''), key='os_namespace')
            workload = st.text_input('Workload', value=os.environ.get('OPENSEARCH_SMOKE_WORKLOAD', ''), key='os_workload')
            lookback = st.selectbox('Zaman aralığı', [15, 30, 60, 240, 1440],
                                    format_func=lambda minutes: f'Son {minutes} dakika' if minutes < 60 else f'Son {minutes // 60} saat', key='os_lookback')
            with st.expander('Gelişmiş Ayarlar'):
                container = st.text_input('Container (isteğe bağlı)', value=os.environ.get('OPENSEARCH_SMOKE_CONTAINER', ''), key='os_container')
                end = st.text_input('Bitiş zamanı (isteğe bağlı, saat dilimli ISO)', key='os_end',
                                   help='Boş bırakılırsa analiz başlangıcı kullanılır. Örnek: 2026-10-04T12:00:00+03:00')
                budget = st.selectbox('En fazla kayıt', [100, 300, 1000, 5000, 10000], index=1, key='os_budget',
                                      help='Analize alınabilecek kayıt sayısını sınırlar; sürekli izleme başlatmaz.')
            page_size = min(openshift.MAX_PAGE_SIZE, max(100, math.ceil(budget / openshift.MAX_PAGES)))
            max_pages = math.ceil(budget / page_size)
            if connection:
                try:
                    request = openshift.make_request(namespace, workload, container, lookback, page_size, max_pages, end)
                except openshift.ApplicationError:
                    st.info('Namespace, workload ve zaman aralığını tamamlayın.')
            target = redact(f'{namespace} / {workload}')
        else:
            upload = st.file_uploader('Dosya veya alarm paketi', type=['zip', 'csv', 'json', 'txt', 'md', 'log'], key='upload')
            st.caption('ZIP: alarm verisi, inventory ve servis bağımlılıkları. Tekil LOG / TXT / MD / JSON / CSV dosyaları da desteklenir.')
            target = redact(upload.name) if upload else ''
        run = st.button('Analizi Başlat', key='run_analysis', type='primary', width='stretch',
                        disabled=request is None if selected_source == OPENSHIFT else upload is None)
    with guide:
        st.subheader('Sonuçtan kanıta')
        st.caption('Önce sistemin ne bulduğunu görün. Ardından pattern, sinyal ve incident kanıtlarını inceleyin.')
        st.markdown('1. Kaynağı ve kapsamı seçin.\n2. Analizi başlatın.\n3. Sonuç ve kanıtları birlikte değerlendirin.')
    if run:
        execute(selected_source, target, upload, request)


def execute(selected_source, target, upload, request):
    lock = get_analysis_lock()
    if not lock.acquire(blocking=False):
        st.warning(openshift.MESSAGES['busy'])
        return
    success = False
    st.session_state.pop('result', None)
    st.session_state.pop('result_source', None)
    try:
        with st.status('Analiz çalışıyor', expanded=True) as status:
            st.write('Veriler alınıyor ve analiz ediliyor. Tamamlandığında sonuç ve karar kanıtları gösterilecek.')
            raw_result = (openshift.run_analysis(get_pipeline, request, automatic=True)
                          if selected_source == OPENSHIFT else run_uploaded(upload))
            model = InvestigationPresenter(redact).build(raw_result, source=selected_source, target=target)
            st.session_state['result'] = model
            st.session_state['result_source'] = selected_source
            st.session_state['new_investigation'] = False
            for key in ('pattern_selection', 'incident_selection', 'pipeline_stage'):
                st.session_state.pop(key, None)
            status.update(label='Analiz tamamlandı', state='complete', expanded=False)
            success = True
    except Exception as error:
        if isinstance(error, openshift.ApplicationError) and error.code == 'no_records':
            st.info(openshift.error_message(error))
        else:
            st.error(openshift.error_message(error))
    finally:
        lock.release()
    if success:
        st.rerun()


if page == 'Overview':
    overview()
elif page == 'Investigations':
    if view and not st.session_state.get('new_investigation', False):
        st.button('Yeni Investigation', key='new_investigation_button', on_click=show_form)
        views.investigation(view)
    else:
        new_investigation()
elif page == 'Log Patterns':
    views.patterns(view)
elif page == 'Incidents':
    views.incidents(view)
