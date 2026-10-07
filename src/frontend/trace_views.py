"""Persisted evidence explorer. No pipeline, source, scoring or RCA execution."""
import json
import os

import streamlit as st

from evidence_redaction import redact_text, SENSITIVE_KEY

STAGES = ('acquisition', 'segmentation', 'parsing', 'patterns', 'signal', 'correlation', 'incident', 'rca')
HISTORICAL = 'Detailed pipeline trace was not stored for this historical run.'
PAGE_SIZE = 100


def safe_payload(value, redact):
    """Defence in depth for every rendering surface, including technical JSON."""
    if isinstance(value, dict):
        return {redact_text(redact(str(k))): '[redacted]' if SENSITIVE_KEY.fullmatch(str(k)) else safe_payload(v, redact)
                for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe_payload(v, redact) for v in value]
    if isinstance(value, str):
        return redact_text(redact(value), (os.environ.get('SAKA_API_KEY'),))
    return value


def load_trace(result, run_id, redact):
    payload = result.get('detailed_pipeline_trace')
    if not isinstance(payload, dict) or payload.get('run_id') != run_id or payload.get('schema_version') != 1:
        return None
    return safe_payload(payload, redact)


def index_trace(trace):
    return {item['ref']: (stage, item) for stage in STAGES for item in trace['stages'][stage]}


def mapping(value):
    # A depth/field budget can replace a nested object with an omission marker.
    return value if isinstance(value, dict) else {}


def filter_items(items, query='', decision='All'):
    words = query.casefold().split()
    return [item for item in items
            if all(word in json.dumps(item, ensure_ascii=False).casefold() for word in words)
            and (decision == 'All' or item['data'].get('qualified') is (decision == 'QUALIFIED'))]


def lineage(trace, ref):
    """Traverse persisted parent links only. Never infer membership from text/IDs."""
    index = index_trace(trace)
    selected = {ref}
    pending = [ref]
    while pending:
        current = pending.pop()
        for parent in index.get(current, ('', {}))[1].get('parents', []):
            if parent not in selected:
                selected.add(parent)
                pending.append(parent)
    descendants = {ref}
    children = {}
    for key, (_, item) in index.items():
        for parent in item['parents']:
            children.setdefault(parent, []).append(key)
    pending = [ref]
    while pending:
        for child in children.get(pending.pop(), []):
            if child not in descendants:
                descendants.add(child)
                pending.append(child)
    selected |= descendants
    rows = {stage: [item for item in trace['stages'][stage] if item['ref'] in selected] for stage in STAGES}
    signals = rows['signal']
    later = any(rows[stage] for stage in ('correlation', 'incident', 'rca'))
    if signals and all(item['data'].get('qualified') is False for item in signals) and not later:
        terminal = 'Suppressed at Signal stage.'
    elif not trace['trace_complete']:
        terminal = 'Trace coverage is limited; absent relationships may not have been stored.'
    elif rows['segmentation'] and all(item['data'].get('admitted') is False for item in rows['segmentation']):
        terminal = 'Not admitted to parsing by the recorded segmentation/window disposition.'
    else:
        terminal = 'End of recorded path. Later objects are shown only when produced.'
    return rows, terminal


def summary_row(item):
    data = item['data']
    canonical = mapping(data.get('canonical'))
    resource = mapping(canonical.get('resource'))
    stream = mapping(data.get('stream'))
    row = {'Ordinal': item['ordinal'], 'Reference': item['ref']}
    values = {
        'Event ID': canonical.get('event_id') or data.get('event_id'),
        'Pattern ID': data.get('template_id'), 'Service': data.get('service_name') or resource.get('service'),
        'Severity': canonical.get('severity') or canonical.get('severity_text') or data.get('severity_min'),
        'Pod': stream.get('pod'), 'Container': stream.get('container'),
        'Boundary status': data.get('boundary_status'), 'Emission reason': data.get('emission_reason'),
        'Delivery': data.get('delivery'), 'Disposition': data.get('disposition'),
        'Admitted': data.get('admitted'), 'Count': data.get('count') or data.get('physical_record_count'),
        'Decision': ('QUALIFIED' if data['qualified'] else 'SUPPRESSED') if 'qualified' in data else None,
        'Backend reason': data.get('qualification_reason'), 'RCA evidence': data.get('kind'),
        'Inside window': data.get('inside_window'), 'Overlap': data.get('acquisition_overlap'),
        'Source time': data.get('source_timestamp') or data.get('source_start_timestamp'),
    }
    row.update({k: v for k, v in values.items() if v is not None})
    text = data.get('text') or canonical.get('message') or data.get('template')
    if text:
        row['Preview'] = text[:240] + (' … [table preview shortened]' if len(text) > 240 else '')
    if item.get('preview_truncated') or item.get('omitted_fields') or item.get('omitted_links'):
        row['Coverage'] = 'Stored evidence shortened'
    return row


def fields(value, prefix=''):
    """Readable field/value rows for actual contract fields, not Python reprs."""
    if isinstance(value, dict):
        for key, child in value.items():
            if key in ('text', 'raw', 'message', 'template'):
                continue
            yield from fields(child, f'{prefix}.{key}' if prefix else key)
    elif isinstance(value, (list, tuple)):
        for i, child in enumerate(value):
            yield from fields(child, f'{prefix}[{i + 1}]')
    else:
        yield {'Field': prefix.replace('_', ' '), 'Recorded value': '' if value is None else str(value)}


def item_detail(trace, item, *, key, related=True):
    index = index_trace(trace)
    st.text('Selected item: ' + item['ref'])
    if item.get('preview_truncated') or item.get('omitted_fields') or item.get('omitted_links'):
        st.warning(f"Stored evidence is shortened: text truncated={item.get('preview_truncated', False)}, "
                   f"omitted fields={item.get('omitted_fields', 0)}, omitted links={item.get('omitted_links', 0)}.")
    st.markdown('**Source input**')
    for parent in item['parents'][:20]:
        if parent in index:
            source = index[parent][1]
            st.text(parent)
            data = source['data']
            content = data.get('text') or mapping(data.get('canonical')).get('message') or data.get('template')
            if content:
                st.code(content, language=None)
            else:
                st.dataframe([summary_row(source)], hide_index=True, width='stretch')
            if source.get('preview_truncated'):
                st.caption('Source text was truncated before persistence.')
        else:
            st.caption(parent + ' — detail unavailable within stored trace coverage.')
    if len(item['parents']) > 20:
        st.caption(f"Showing 20 of {len(item['parents'])} source inputs; all stored links are available below.")
    st.markdown('**Stage output / recorded evidence**')
    data = item['data']
    if data.get('kind') == 'deterministic':
        st.info('Deterministic RCA conclusion and ranked evidence. Correlation alone does not prove causality.')
    elif str(data.get('kind', '')).startswith('expert'):
        st.info('Optional expert interpretation / submitted evidence. Deterministic RCA remains authoritative.')
    for text in (data.get('text'), mapping(data.get('canonical')).get('message'), data.get('template')):
        if text:
            st.code(text, language=None)
    rows = list(fields(data))
    if rows:
        st.dataframe(rows[:200], hide_index=True, width='stretch')
        if len(rows) > 200:
            st.caption(f'Showing 200 of {len(rows)} recorded fields. Full stored fields are in Technical evidence.')
    children = [ref for ref, (_, row) in index.items() if item['ref'] in row['parents']]
    if related:
        path, _ = lineage(trace, item['ref'])
        choices = [row['ref'] for stage in STAGES for row in path[stage] if row['ref'] != item['ref']]
        if choices:
            selected = st.selectbox('Inspect related input / next output', choices, index=None,
                                    format_func=lambda ref: index[ref][0].title() + ' · ' + ref, key=key + '_related')
            if selected:
                with st.container(border=True):
                    item_detail(trace, index[selected][1], key=key + '_linked', related=False)
    with st.expander('Technical evidence'):
        st.json(dict(item, next_outputs=children))


def stage_evidence(trace, stage, *, key):
    st.subheader('Records / Evidence')
    if trace is None:
        st.info(HISTORICAL)
        return
    items = trace['stages'][stage]
    total = trace['totals'][stage]
    st.caption(f"Stored {len(items)} of {total} {stage} items; omitted items: {trace['omitted_items'][stage]}. "
               'Evidence belongs to this completed run; opening or filtering it does not run analysis.')
    if not trace['trace_complete']:
        st.warning('Trace coverage is limited. Item, text, field or byte limits shortened stored evidence.')
    if not items:
        st.info(f'No {stage} objects were produced in this run.' if total == 0 else 'Details omitted by the trace budget.')
        return
    query = st.text_input('Search evidence (ID, text, severity, service, pod, pattern, boundary status)', key=key + '_search')
    decision = st.selectbox('Signal decision', ['All', 'QUALIFIED', 'SUPPRESSED'], key=key + '_decision') if stage == 'signal' else 'All'
    matches = filter_items(items, query, decision)
    if stage == 'patterns':
        pattern = st.selectbox('Inspect pattern occurrences', ['All'] + sorted({item['data']['template_id'] for item in items
                                                                               if item['data'].get('template_id')}),
                               key=key + '_pattern')
        if pattern != 'All':
            matches = [item for item in matches if item['data'].get('template_id') == pattern]
    if not matches:
        st.info('No stored items match these filters.')
        return
    page = st.selectbox('Evidence page', range((len(matches) + PAGE_SIZE - 1) // PAGE_SIZE),
                        format_func=lambda n: str(n + 1), key=key + '_page')
    visible = matches[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    st.caption(f'Showing {len(visible)} of {len(matches)} matching stored items. Page size: {PAGE_SIZE}.')
    st.dataframe([summary_row(item) for item in visible], hide_index=True, width='stretch')
    selected = st.selectbox('Inspect evidence item', [item['ref'] for item in visible], index=None, key=key + '_item')
    if selected:
        st.subheader('Selected Item Details')
        item_detail(trace, next(item for item in visible if item['ref'] == selected), key=key)


def event_lineage(trace, *, key):
    st.subheader('Event Lineage')
    if trace is None:
        st.info(HISTORICAL)
        return
    query = st.text_input('Find source / logical event', key=key + '_lineage_search')
    items = filter_items(trace['stages']['acquisition'] + trace['stages']['segmentation'], query)
    if not items:
        st.info('No stored source or logical events match.')
        return
    selected = st.selectbox('Follow one event', [item['ref'] for item in items], index=None, key=key + '_lineage')
    if selected is None:
        return
    rows, terminal = lineage(trace, selected)
    st.info(terminal)
    for stage in STAGES:
        st.markdown('**' + stage.title() + '**')
        if rows[stage]:
            st.dataframe([summary_row(item) for item in rows[stage]][:PAGE_SIZE], hide_index=True, width='stretch')
            if len(rows[stage]) > PAGE_SIZE:
                st.caption(f'Showing {PAGE_SIZE} of {len(rows[stage])} linked items; use stage evidence to inspect the remainder.')
        else:
            st.caption('None recorded on this path.' if trace['trace_complete'] else 'No stored link; trace coverage is limited.')
    linked = [item['ref'] for stage in STAGES for item in rows[stage]]
    detail = st.selectbox('Inspect item on this path', linked, key=key + '_lineage_detail')
    item_detail(trace, index_trace(trace)[detail][1], key=key + '_lineage_item')
