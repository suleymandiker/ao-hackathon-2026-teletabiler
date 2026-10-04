"""Streamlit renderers consume presentation models, never raw backend payloads."""
import json

import streamlit as st

import theme
from presentation import EVIDENCE_LIMIT, TIMELINE_LIMIT


def html(markup):
    st.markdown(markup, unsafe_allow_html=True)


def details(rows):
    if rows:
        st.dataframe([{'Gösterge': label, 'Değer': value} for label, value in rows], hide_index=True, width='stretch')


def evidence(items):
    with st.expander('Kanıtları Göster'):
        if items:
            st.caption(f'En fazla {EVIDENCE_LIMIT} temsilci olay referansı. Ham kaynak mesajları gösterilmez.')
            st.dataframe([{'Olay referansı': item} for item in items], hide_index=True, width='stretch')
        else:
            st.caption('Bu sonuçta gösterilebilir olay referansı bulunmuyor.')


def signals(view):
    stage = next(stage for stage in view.stages if stage.key == 'signal')
    details(stage.details)
    if view.reason_counts:
        st.dataframe([{'Backend değerlendirmesi': label, 'Sinyal sayısı': count} for label, count in view.reason_counts],
                     hide_index=True, width='stretch')
    st.caption('Gösterilen nedenler backend kararlarıdır. Ayrıntılı eşik veya zaman etkisi dağılımı türetilmez.')
    if view.signals:
        with st.expander('Sinyal karar kanıtları'):
            st.dataframe([{'Servis': item.service, 'Olay': item.count, 'Sonuç': item.outcome,
                           'Değerlendirme': item.reason, 'Karar kanıtı': ' · '.join(item.evidence)} for item in view.signals],
                         hide_index=True, width='stretch')


def patterns(view):
    st.title('Log Patterns')
    html(theme.intro('Bu analizde bulunan tekrar eden log kalıpları.'))
    if not view or not view.patterns:
        html(theme.empty('Gösterilecek pattern yok', 'Bir investigation tamamlandığında desteklenen pattern sonuçları burada görünür.'))
        return
    st.caption(f'Bu investigation · en fazla {view.display_limit} pattern gösterilir.')
    rows = []
    for item in view.patterns:
        row = {'Pattern': item.text, 'Olay sayısı': item.count, 'Seviye': item.severity, 'Sonuç': item.outcome}
        if item.first_seen:
            row['İlk gözlem (UTC)'] = item.first_seen
        if item.last_seen:
            row['Son gözlem (UTC)'] = item.last_seen
        rows.append(row)
    st.dataframe(rows, width='stretch', hide_index=True)
    selected = st.selectbox('Pattern incele', range(len(view.patterns)), index=None,
                            format_func=lambda index: view.patterns[index].text[:100], key='pattern_selection',
                            placeholder='Ayrıntısını görmek için bir pattern seçin')
    if selected is None:
        return
    item = view.patterns[selected]
    with st.container(border=True):
        html(theme.badge(item.outcome, 'attention' if item.outcome == 'Suppressed' else 'success' if item.outcome == 'Qualified' else 'info'))
        html(theme.pattern(item.text))
        details(tuple((label, str(value)) for label, value in
                      [('Olay sayısı', item.count), ('Seviye', item.severity), ('İlk gözlem (UTC)', item.first_seen),
                       ('Son gözlem (UTC)', item.last_seen), ('Servisler', ', '.join(item.services)), ('Hostlar', ', '.join(item.hosts))]
                      if value is not None and value != ''))
        st.caption('Pattern → Signal Candidate → ' + item.outcome)
        for signal in item.signals:
            if signal.reason:
                st.text(signal.reason)
        evidence(item.evidence)
        with st.expander('Teknik Ayrıntılar'):
            details((('Pattern ID', item.pattern_id),))
            for signal in item.signals:
                details((('Signal ID', signal.signal_id),) + signal.technical)
                if signal.evidence:
                    st.text(' · '.join(signal.evidence))


def topology(incident):
    if not incident.topology:
        return
    st.subheader('Servis bağımlılıkları')
    names = sorted({name for path in incident.topology for name in path})
    ids = {name: f'n{number}' for number, name in enumerate(names)}
    graph = ['digraph dependencies { rankdir=LR; bgcolor="transparent"; node [shape=box, style="rounded,filled", fontname="Arial", color="#cfdaea", fillcolor="#eff5ff"];']
    for name in names:
        color = '#fff0cc' if name == incident.root_entity else '#ffecef' if name in incident.services else '#eef1f5'
        graph.append(f'{ids[name]} [label={json.dumps(name)}, fillcolor="{color}"];')
    for path in incident.topology:
        for start, end in zip(path, path[1:]):
            graph.append(f'{ids[start]} -> {ids[end]};')
    graph.append('}')
    st.graphviz_chart('\n'.join(graph), width='stretch')
    st.caption('Kehribar: olası kök neden · Kırmızı: incident servisleri · Gri: ek bağlam')
    st.caption('Paket bağlamındaki tanımlı bağımlılıklar gösterilir. İlişki tek başına nedensellik kanıtı değildir.')


def incident_detail(incident):
    with st.container(border=True):
        html(theme.badge(incident.severity or 'Incident adayı', incident.tone))
        st.subheader(incident.title)
        st.caption(incident.incident_id)
        left, right = st.columns([1.3, 1])
        with left:
            st.markdown('**Olası kök neden**')
            st.text(incident.root or 'Bu sonuçta tek bir kök neden belirlenmedi.')
            st.caption('Incident ve RCA çıktıları inceleme gerektiren hipotezlerdir.')
        with right:
            st.markdown('**Etkilenen servisler**')
            st.text(', '.join(incident.services) or 'Servis bilgisi bulunmuyor.')
        details(incident.metrics)
        topology(incident)
        if incident.hypotheses:
            with st.expander('RCA hipotezlerini incele', expanded=True):
                for label, reason in incident.hypotheses:
                    html(theme.pattern(label))
                    if reason:
                        st.text(reason)
        evidence(incident.evidence)
        with st.expander('Teknik Ayrıntılar'):
            details(incident.technical)


def incidents(view):
    st.title('Incidents')
    html(theme.intro('Aktif investigation içindeki incident adayları ve olası kök nedenler.'))
    if not view or not view.incidents:
        html(theme.empty('Incident bulunmuyor', 'Bu oturumda seçili investigation için gösterilebilir bir incident adayı yok.'))
        return
    selected = st.selectbox('Incident incele', range(len(view.incidents)),
                            format_func=lambda index: view.incidents[index].title, key='incident_selection')
    incident_detail(view.incidents[selected])


def stage_details(view):
    selected = st.selectbox('Pipeline aşamasını incele', range(len(view.stages)),
                           format_func=lambda index: view.stages[index].name, key='pipeline_stage')
    stage = view.stages[selected]
    with st.container(border=True):
        st.subheader(stage.name)
        st.caption(stage.description)
        if stage.key == 'signal':
            signals(view)
        else:
            details(stage.details)
        if stage.key == 'patterns' and view.patterns:
            st.dataframe([{'Pattern': item.text, 'Olay': item.count, 'Sonuç': item.outcome} for item in view.patterns],
                         hide_index=True, width='stretch')
        elif stage.key == 'correlation' and view.correlations:
            st.dataframe([{'Kaynak servis': source, 'Hedef servis': target, 'Kanıt': reason}
                          for source, target, reason in view.correlations], hide_index=True, width='stretch')
            st.caption('Korelasyon, tek başına nedensellik kanıtı değildir.')
        elif stage.key in ('incident', 'rca') and view.incidents:
            for incident in view.incidents:
                incident_detail(incident)


def investigation(view):
    st.title('Investigation sonucu')
    st.caption(view.source + (' · ' + view.target if view.target else ''))
    html(theme.summary(view))
    if view.incidents:
        incident_detail(view.incidents[0])
    st.subheader('Analiz nasıl bu sonuca ulaştı?')
    html(theme.pipeline(view.stages))
    columns = st.columns([1, 1.35, 1.25])
    with columns[0], st.container(border=True):
        st.subheader('Temel göstergeler')
        details(view.counts)
    with columns[1], st.container(border=True):
        st.subheader('En sık görülen pattern')
        if view.patterns:
            item = view.patterns[0]
            html(theme.pattern(item.text))
            st.caption(f'{item.count} olay · {item.severity} · {item.outcome}')
        else:
            st.caption('Bu sonuçta pattern bilgisi yok.')
    with columns[2], st.container(border=True):
        st.subheader('Sinyal değerlendirmesi')
        signal_stage = next(stage for stage in view.stages if stage.key == 'signal')
        details(signal_stage.details)
        for reason, count in view.reason_counts:
            st.text(f'{count} · {reason}')
    stage_details(view)
    if view.timeline:
        with st.expander('Zaman çizelgesi'):
            st.caption(f'Gerçek kaynak zamanlarından türetilen en fazla {TIMELINE_LIMIT} gözlem; işlem aşaması zamanları değildir.')
            st.dataframe([{'Zaman (UTC)': item.timestamp, 'Gözlem': item.label} for item in view.timeline],
                         hide_index=True, width='stretch')
    if view.expert_note:
        st.info(view.expert_note)
    if view.expert_commentary:
        with st.expander('Uzman yorumu'):
            st.caption('Mevcut backend kanıtlarının yorumu; deterministik kararların veya doğrulanmış nedenselliğin yerine geçmez. İnceleme adımları öneridir.')
            details(view.expert_commentary)
    with st.expander('Teknik Ayrıntılar'):
        details(view.technical)
        st.caption('Tek bir sonlu analiz. Ayrıntı tabloları ve kanıtlar gösterim sınırına tabidir; ana sayımlar tam sonuçtan hesaplanır.')
