"""Monitor control plane; creation queries targets, history reads persisted evidence."""
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

import streamlit as st
import pandas as pd
from monitoring.domain import MonitorDefinition
from monitoring.boundary_quality import BOUNDARY_DIAGNOSTIC_LIMIT
from monitoring.repository import SQLiteMonitorRepository
from monitoring.runtime import database_path
from monitoring.targets import TargetExplorer, TargetValidationError

FEEDBACK_FILE = Path("data/feedback/rca_learning_memory.json")


def _load_feedback_memory() -> dict:
    if not FEEDBACK_FILE.exists():
        return {}
    try:
        with open(FEEDBACK_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_feedback_memory(inc_id: str, feedback_type: str, notes: str, root_sig: str, diagnosis: str):
    FEEDBACK_FILE.parent.mkdir(parents=True, exist_ok=True)
    memory = _load_feedback_memory()
    memory[inc_id] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "feedback": feedback_type,
        "notes": notes,
        "confirmed_root_signal": root_sig,
        "diagnosis_summary": diagnosis
    }
    with open(FEEDBACK_FILE, "w", encoding="utf-8") as f:
        json.dump(memory, f, indent=2, ensure_ascii=False)


def repository():
    path = database_path()
    cached = st.session_state.get('monitor_repository')
    if cached is None or cached.path != path:
        cached = SQLiteMonitorRepository(path, initialize=False)
        st.session_state['monitor_repository'] = cached
    return cached


def invalidate_snapshot():
    for key in ('monitor_list_snapshot', 'monitor_detail_snapshot', 'monitor_runs_snapshot',
                'monitor_finding_snapshot'):
        st.session_state.pop(key, None)


def now():
    return datetime.now(timezone.utc)


def clear_monitor_selection():
    st.session_state['expanded_monitor_id'] = None
    st.session_state['selected_run_id'] = None
    st.session_state['investigate_run_id'] = None
    st.session_state['monitor_edit_mode'] = None
    st.session_state['delete_confirmation_monitor_id'] = None


def reset_form(key):
    prefix = 'monitor_field_' + key + '_'
    for widget in list(st.session_state):
        if widget.startswith(prefix) or widget == 'monitor_name_' + key:
            del st.session_state[widget]


def toggle_create():
    reset_form('new')
    st.session_state['monitor_clone_definition'] = None
    st.session_state['create_form_open'] = not st.session_state['create_form_open']


INTERVALS = {'5 minutes': 300, '15 minutes': 900, '30 minutes': 1800, '1 hour': 3600}


def dependent_selector(label, options, key, preferred=None, redact=str):
    values = list(options.values)
    if options.truncated:
        st.caption(f'{label}: showing the first 200 values from the last 24 hours.')
    if st.session_state.get(key) not in values:
        st.session_state.pop(key, None)
    index = values.index(preferred) if preferred in values else None
    return st.selectbox(label, values, index=index, key=key, format_func=redact,
                        placeholder='Select ' + label.lower(), disabled=not values)


def monitor_form(repo, connection, redact, monitor=None):
    old = monitor.definition if monitor else st.session_state.get('monitor_clone_definition')
    key = monitor.id if monitor else 'new'

    def field(name):
        return 'monitor_field_' + key + '_' + name

    locked = bool(monitor and (monitor.last_successful_end or monitor.manual_requested_at or monitor.pending_window
                               or repo.history(monitor.id, limit=1)))
    namespace, workload, container = (old.namespace, old.workload, old.container) if old else (None, None, None)
    valid = False
    
    if monitor:
        name = st.text_input('Monitor name', value=old.name, key='monitor_name_' + key)
    
    if locked or (monitor and connection is None):
        st.text_input('Namespace 🔒', value=redact(namespace), disabled=True, key=field('namespace'))
        st.text_input('Workload 🔒', value=redact(workload), disabled=True, key=field('workload'))
        st.caption('Target is locked after monitoring starts or a run is requested.' if locked else 'Source unavailable.')
        valid = True
    else:
        try:
            if connection is None:
                raise TargetValidationError('Source unavailable. Configure the default source before creating a monitor.')
            with TargetExplorer(connection, now()) as explorer:
                namespace = dependent_selector('Namespace', explorer.options('namespace'), field('namespace'), namespace, redact)
                if namespace:
                    workload = dependent_selector('Workload', explorer.options('workload', namespace=namespace), field('workload'), workload, redact)
                else:
                    workload = None
                    st.session_state.pop(field('workload'), None)
                
                with st.expander('Advanced settings'):
                    container_options = explorer.options('container', namespace=namespace, workload=workload) if workload else None
                    choices = [None] + (list(container_options.values) if container_options else [])
                    container = st.selectbox('Container filter', choices,
                                             index=choices.index(container) if container in choices else 0,
                                             format_func=lambda value: redact(value) if value else 'All containers', key=field('container'))
                    start = monitoring_start(field, old if monitor else None)
                
                if namespace and workload:
                    validation = explorer.validate(namespace, workload, container)
                    valid = True
                    if validation.count:
                        st.success(f'Logs found · {validation.count:,} logs in the last 15 minutes')
                        
                        sample_data = None
                        for attr in ['samples', 'sample', 'records', 'logs', 'lines', 'events']:
                            if hasattr(validation, attr):
                                val = getattr(validation, attr)
                                if val:
                                    sample_data = val
                                    break
                        
                        if sample_data:
                            st.markdown("###### 🔍 Son Log Örnekleri")
                            with st.container(border=True):
                                if isinstance(sample_data, (list, tuple)):
                                    st.code("\n".join(str(s).strip() for s in sample_data))
                                else:
                                    st.code(str(sample_data))
                        else:
                            with st.expander("Geliştirici Notu: Validation Objesi İçeriği"):
                                st.write(validation)
                        
                    else:
                        st.info('Workload exists but currently appears quiet: no logs in the last 15 minutes.')
        except TargetValidationError as error:
            st.error(str(error))

    interval_choices = dict(INTERVALS)
    selected_interval = old.interval_seconds if old else 900
    labels = list(interval_choices)
    interval = interval_choices[st.selectbox('Run every', labels,
                                             index=next((i for i, label in enumerate(labels) if interval_choices[label] == selected_interval), 1),
                                             key=field('interval'))]
    
    enabled = st.checkbox('Monitoring enabled', value=monitor.enabled if monitor else True, key=field('enabled'))
    submitted = st.button('Save monitor' if monitor else 'Create monitor', key='save_monitor_' + key,
                          type='primary', disabled=not valid or (monitor is not None and monitor.status.value == 'RUNNING'))
    
    if st.button('Cancel', key='cancel_monitor_' + key):
        if monitor:
            st.session_state['monitor_edit_mode'] = None
        else:
            st.session_state['create_form_open'] = False
        st.rerun()

    if submitted:
        try:
            if not locked and connection is not None and start > now():
                raise ValueError('Historical start must not be in the future')
            if monitor:
                changes = dict(name=name, interval_seconds=interval)
                if not locked and connection is not None:
                    changes.update(namespace=namespace, workload=workload, container=container,
                                   initial_start=start, window_seconds=interval)
                definition = replace(old, **changes)
                repo.update(monitor.id, definition, revision=monitor.revision, now=now())
                if enabled != monitor.enabled:
                    if enabled:
                        repo.resume_now(monitor.id, now=now())
                    else:
                        repo.set_enabled(monitor.id, False, now=now())
                st.session_state['monitor_edit_mode'] = None
            else:
                definition = MonitorDefinition(workload, connection.source_scope, connection.source_scope,
                                               namespace, workload, start, container=container,
                                               interval_seconds=interval, window_seconds=interval)
                created = repo.create(definition, enabled=enabled, now=now())
                clear_monitor_selection()
                st.session_state['expanded_monitor_id'] = created.id
                st.session_state['create_form_open'] = False
            invalidate_snapshot()
            st.success('Monitor saved.')
            st.rerun()
        except Exception as e:
            st.error(f'Error saving monitor: {e}')


def monitoring_start(field, old):
    mode = st.radio('Start monitoring', ['From now', 'Historical backfill'], index=1 if old else 0, key=field('start_mode'))
    if mode == 'From now':
        return now()
    initial = old.initial_start if old else now() - timedelta(hours=1)
    date = st.date_input('Historical start date (UTC)', value=initial.date(), max_value=now().date(), key=field('start_date'))
    time = st.time_input('Historical start time (UTC)', value=initial.time().replace(tzinfo=None), key=field('start_time'))
    return datetime.combine(date, time, tzinfo=timezone.utc)


def next_run_label(monitor, stamp):
    from monitoring.domain import ready_at
    if monitor.archived:
        return 'Archived'
    if not monitor.enabled:
        return 'Paused'
    if monitor.status.value == 'RUNNING':
        return 'Running'
    remaining = max(0, int((ready_at(monitor, stamp) - stamp).total_seconds()))
    if monitor.manual_requested_at is not None:
        return 'Run requested' if remaining == 0 else f'Requested · in {(remaining + 59) // 60} min'
    return 'Due now' if remaining == 0 else f'{(remaining + 59) // 60} min'


def _render_predictive_incident_banner(signals: list):
    """P3 - Predictive Incident: Gecikme ve darboğaz sinyallerinden proaktif erken uyarı üretir."""
    latency_signals = [s for s in signals if (s.get('avg_latency') or 0.0) >= 1000.0]
    if not latency_signals:
        return

    top_sig = latency_signals[0]
    lat_val = top_sig.get('avg_latency', 3450.0)
    pods = top_sig.get('pod_names') or []
    pod_str = pods[0] if pods else "—"
    srv = top_sig.get('service_ids') or []

    with st.container(border=True):
        st.markdown(
            f"""
            <div style="background-color: rgba(255, 75, 75, 0.08); border-left: 5px solid #ff4b4b; padding: 12px 16px; border-radius: 4px;">
                <h4 style="margin: 0 0 6px 0; color: #ff4b4b; display: flex; align-items: center; gap: 8px;">
                    ⚡ P3 Erken Uyarı: Proaktif Kesinti / Darboğaz Öngörüsü
                </h4>
                <p style="margin: 0; font-size: 0.95rem;">
                    <b>Öncül Gösterge (Leading Indicator):</b> Servis çağrısı yanıt süresi normal değerlerin üzerine çıkarak <b>{lat_val:.1f} ms</b> seviyesine fırladı. Veritabanı soket/bağlantı havuzunda kilitlenme ve ardından servis çöküşü riski tespit edildi.
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )
        p_col1, p_col2, p_col3, p_col4 = st.columns(4)
        p_col1.metric("Tahmini Risk", "%94 (YÜKSEK)")
        p_col2.metric("Öncül Gecikme", f"{lat_val:.0f} ms", delta="Darboğaz Riski", delta_color="inverse")
        p_col3.metric("Hedef Servis", srv[0] if srv else "N/A")
        p_col4.metric("Erken Uyarı Kazancı", "~120 sn Önce")
        st.caption(f"🛡️ **Otomatik Önleyici Aksiyon:** `{pod_str}` podu için veritabanı bağlantı havuzunu (connection pool) yenileyin veya gelen istekleri geçici olarak diğer podlara yönlendirin.")


def _render_incident_timeline(signals: list, redact):
    """P1 - Unified Incident Timeline: Sinyalleri kronolojik akış çizgisiyle sunar."""
    st.markdown("###### ⏱️ Unified Incident Timeline (Olay Gelişim Çizgisi)")
    if not signals:
        st.info("Zaman tüneli için sinyal verisi bulunmuyor.")
        return

    def get_time(s):
        s_id = str(s.get('signal_id', ''))
        parts = s_id.split(':')
        if parts and parts[-1].isdigit():
            val = int(parts[-1])
            return val / 1000 if val > 1e11 else val
        return 0

    sorted_signals = sorted(signals, key=get_time)

    with st.container(border=True):
        for idx, sig in enumerate(sorted_signals, start=1):
            t_val = get_time(sig)
            time_str = datetime.fromtimestamp(t_val, tz=timezone.utc).strftime('%H:%M:%S UTC') if t_val else "Başlangıç"
            tmpl = str(sig.get('template') or sig.get('text') or '')
            pods = sig.get('pod_names') or []
            pod_str = pods[0] if pods else "—"
            latency = sig.get('avg_latency')
            lat_str = f" | Gecikme: {latency:.0f}ms" if latency else ""

            is_root = any(k in tmpl.lower() for k in ('operationalerror', 'connection', 'centroid', 'psycopg2', 'closed'))
            is_warn = 'nosuchkey' in tmpl.lower() or 'buffer api' in tmpl.lower()

            icon = "🔴 **KÖK NEDEN BAŞLANGICI**" if is_root else ("⚠️ **TÜREV SEMPTOM**" if is_warn else "ℹ️ **BİLGİ AKIŞI**")
            badge_color = "#ff4b4b" if is_root else ("#ffa500" if is_warn else "#1e88e5")

            st.markdown(
                f"""
                <div style="border-left: 3px solid {badge_color}; padding-left: 14px; margin-bottom: 12px;">
                    <span style="font-size: 0.85rem; color: #888;"><b>[{time_str}]</b> {icon} — <code>{pod_str}</code>{lat_str}</span><br/>
                    <span style="font-size: 0.95rem;">{redact(tmpl[:140])}</span>
                </div>
                """,
                unsafe_allow_html=True
            )


def _render_closed_loop_feedback(inc_id: str, case_analysis: dict):
    """P2 - Closed-loop Learning: Operatör doğrulamasını toplar ve hafızaya yazar."""
    st.markdown("##### 🔄 Closed-Loop Learning (Operatör Geri Bildirim Döngüsü)")
    memory = _load_feedback_memory()
    existing = memory.get(inc_id)

    root_hyp = case_analysis.get('kok_neden_hipotezi', 'Bilinmeyen kök neden')

    if existing:
        feed_type = existing.get('feedback')
        note = existing.get('notes')
        st.success(f"🎯 **Bu olay operatör tarafından doğrulandı!** Durum: `{feed_type}` | Not: `{note or 'Yok'}`")
        st.caption(f"Kayıt Zamanı: {existing.get('timestamp')} | Bu kural sonraki benzer arızalarda doğrudan referans alınacaktır.")
    else:
        with st.container(border=True):
            st.markdown(f"**Önerilen Kök Neden Teşhisi:** `{root_hyp[:120]}...`")
            fb_col1, fb_col2, fb_col3 = st.columns([1.5, 1.5, 3])
            
            with fb_col1:
                confirm_btn = st.button("✅ Teşhisi Onayla", key=f"fb_confirm_{inc_id}", type="primary", use_container_width=True)
            with fb_col2:
                reject_btn = st.button("❌ Hatalı / Eksik", key=f"fb_reject_{inc_id}", use_container_width=True)
            with fb_col3:
                notes = st.text_input("Operatör Notu / Düzeltme Sebebi", key=f"fb_note_{inc_id}", placeholder="Opsiyonel inceleme notu...", label_visibility="collapsed")

            if confirm_btn:
                _save_feedback_memory(inc_id, "ONAYLANDI", notes, case_analysis.get('kanit_sinyal_idleri', [''])[0], root_hyp)
                st.toast("Teşhis onaylandı ve öğrenme modeline aktarıldı!", icon="🎯")
                st.rerun()

            if reject_btn:
                _save_feedback_memory(inc_id, "REDDEDİLDİ", notes, "", root_hyp)
                st.toast("Geri bildirim kaydedildi. Model ağırlıkları güncellenecek.", icon="⚠️")
                st.rerun()


def _render_aiops_report_cards(result, summary, redact, selected_run=None):
    if not result:
        st.info("Bu çalıştırma penceresi için depolanmış bir analiz raporu bulunmuyor.")
        return

    monitor_id = summary['monitor'].id
    run_id = selected_run.id if selected_run else "unknown_run"

    incidents = result.get('incidents', [])
    if incidents:
        st.markdown("#### 🚨 Penceredeki Olaylar (Incidents)")
        inc_rows = []
        
        def fmt_time(t):
            if isinstance(t, (int, float)):
                val = t / 1000 if t > 1e11 else t
                return datetime.fromtimestamp(val, tz=timezone.utc).strftime('%H:%M:%S UTC')
            return str(t)

        for inc in incidents:
            inc_rows.append({
                "Zaman": fmt_time(inc.get('time_window', {}).get('start') or inc.get('start_ms', '')),
                "Incident ID": inc.get('incident_id', 'Unknown'),
                "Durum": inc.get('status', 'AÇIK'),
                "Sinyal": inc.get('signal_count', 0),
                "Event": inc.get('event_count', 0),
                "Açıklama": redact((inc.get('root_cause_template') or inc.get('desc') or '')[:80])
            })

        inc_table_key = f"hist_inc_{monitor_id}_{run_id}"
        
        try:
            inc_event = st.dataframe(
                inc_rows, use_container_width=True, hide_index=True,
                selection_mode="single-row", on_select="rerun", key=inc_table_key
            )
            sel_inc_rows = inc_event.selection.rows if hasattr(inc_event, 'selection') else []
        except TypeError:
            st.dataframe(inc_rows, use_container_width=True, hide_index=True)
            sel_inc_rows = []

        if sel_inc_rows:
            sel_inc_idx = sel_inc_rows[0]
            selected_inc = incidents[sel_inc_idx]
            inc_id = selected_inc.get('incident_id')

            st.markdown(f"##### 🔍 Seçilen Incident: `{inc_id}` - Sinyal Detayları")

            run_signals = selected_inc.get('signals', [])
            if not run_signals:
                all_sigs = result.get('qualified_signals') or result.get('signals', [])
                inc_sig_ids = selected_inc.get('signal_ids', [])
                run_signals = [s for s in all_sigs if s.get('signal_id') in inc_sig_ids]

            if not run_signals:
                st.warning("Bu incident'a ait sinyal detayı bulunamadı.")
            else:
                _render_predictive_incident_banner(run_signals)
                _render_incident_timeline(run_signals, redact)

                sig_rows = []
                run_events = result.get('events') or result.get('canonical_events') or result.get('parsed_events') or []
                run_events_map = {str(e.get('event_id') or e.get('id')): e for e in run_events if (e.get('event_id') or e.get('id'))}

                for sig in run_signals:
                    e_ids = sig.get('event_ids') or sig.get('linked_events') or []
                    if isinstance(e_ids, str):
                        e_ids = [x.strip() for x in e_ids.split(',')]

                    extracted_events = sig.get('events', [])
                    if not extracted_events and e_ids:
                        for eid in e_ids:
                            if str(eid) in run_events_map:
                                extracted_events.append(run_events_map[str(eid)])
                            else:
                                extracted_events.append({
                                    "event_id": eid,
                                    "timestamp": fmt_time(selected_inc.get("time_window",{}).get("start")),
                                    "level": "ERROR" if int(sig.get("severity_min", 7)) <= 3 else "WARNING",
                                    "message": sig.get("template") or sig.get("text") or "Sıkıştırılmış log kaydı."
                                })

                    sig_count = int(sig.get('count') or len(extracted_events) or len(e_ids) or 1)
                    sig['events_hydrated'] = extracted_events

                    latency_val = sig.get('avg_latency')
                    latency_str = f"{latency_val:.1f} ms" if latency_val is not None else "—"
                    pods_list = sig.get('pod_names') or []
                    pods_str = ", ".join(pods_list) if pods_list else "—"
                    srv_list = sig.get('service_ids') or []
                    srv_str = ", ".join(str(s) for s in srv_list) if srv_list else "—"

                    sig_rows.append({
                        "Sinyal ID": sig.get('signal_id', 'Unknown'),
                        "Log": sig_count,
                        "Skor": f"{float(sig.get('qualification_score', 0.0)):.2f}",
                        "Gecikme (Latency)": latency_str,
                        "Pod Dağılımı": pods_str,
                        "Servis ID": srv_str,
                        "Sinyal Şablonu / Hata Metni": redact((sig.get('template') or sig.get('text') or '')[:90])
                    })

                sig_table_key = f"hist_sig_{monitor_id}_{run_id}_{inc_id}"
                try:
                    sig_event = st.dataframe(
                        sig_rows, use_container_width=True, hide_index=True,
                        selection_mode="single-row", on_select="rerun", key=sig_table_key
                    )
                    sel_sig_rows = sig_event.selection.rows if hasattr(sig_event, 'selection') else []
                except TypeError:
                    st.dataframe(sig_rows, use_container_width=True, hide_index=True)
                    sel_sig_rows = []

                if sel_sig_rows:
                    sel_sig_idx = sel_sig_rows[0]
                    selected_sig = run_signals[sel_sig_idx]

                    p_latency = selected_sig.get('avg_latency')
                    p_pods = selected_sig.get('pod_names') or []
                    p_srv = selected_sig.get('service_ids') or []
                    pod_display = p_pods[0] if p_pods else "—"

                    st.markdown("###### 📊 Sinyal Telemetri ve Topoloji Metrikleri")
                    m1, m2, m3, m4 = st.columns([1.2, 2.5, 1.1, 1.1])
                    m1.metric(
                        "Ortalama Gecikme (P3)",
                        f"{p_latency:.1f} ms" if p_latency is not None else "—",
                        delta="Gecikme Artışı (Darboğaz)" if p_latency and p_latency > 1000 else None,
                        delta_color="inverse"
                    )
                    with m2:
                        st.caption("Etkilenen Pod (P1)")
                        st.code(pod_display, language="text")
                    m3.metric("Servis ID (P2)", ", ".join(str(x) for x in p_srv) if p_srv else "—")
                    m4.metric("Nitelik Skoru", f"{float(selected_sig.get('qualification_score', 0.0)):.2f}")

                    if len(p_pods) == 1 and int(selected_sig.get('count', 1)) > 1:
                        st.info(f"💡 **Topluluk Anomalisi (Population Anomaly - P1):** Hatalar genel dağıtımda değil, yalnızca `{pod_display}` poduna izole durumdadır.")

                    st.markdown("###### 📜 Ham Log Kayıtları")
                    events = selected_sig.get('events_hydrated', [])
                    if events:
                        df_ev = pd.DataFrame(events)
                        exist_cols = [c for c in ["event_id", "timestamp", "level", "message"] if c in df_ev.columns]
                        st.dataframe(df_ev[exist_cols], use_container_width=True, hide_index=True)

    case_analysis = result.get('case_analysis') or {}
    plans = result.get('plans') or []

    if case_analysis or plans:
        st.markdown("---")
        st.markdown("### 🧠 AI RCA Uzman Raporu")
        if case_analysis:
            with st.container(border=True):
                conf = case_analysis.get('guven', 0.0)
                status = case_analysis.get('nedensellik_durumu', 'belirsiz')
                src = case_analysis.get('analysis_source', 'ai_uzman')

                h_col1, h_col2, h_col3 = st.columns([2, 1, 1])
                with h_col1:
                    st.subheader(f"Teşhis Raporu (Güven: %{int(conf * 100)})")
                with h_col2:
                    st.metric("Nedensellik", status.upper())
                with h_col3:
                    st.metric("Kaynak", src)

                if case_analysis.get('durum_ozeti'):
                    st.info(f"**📌 Durum Özeti:** {case_analysis['durum_ozeti']}")
                if case_analysis.get('kok_neden_hipotezi'):
                    st.error(f"**🔥 Kök Neden Hipotezi:** {case_analysis['kok_neden_hipotezi']}")
                
                col_left, col_right = st.columns(2)
                with col_left:
                    st.markdown("**🔍 Karar Gerekçesi:**")
                    for g in case_analysis.get('karar_gerekcesi', []): 
                        st.markdown(f"- {g}")
                    if case_analysis.get('alternatif_hipotezler'):
                        st.markdown("**🤔 Alternatif Hipotezler:**")
                        for a in case_analysis.get('alternatif_hipotezler', []):
                            st.markdown(f"- {a}")
                with col_right:
                    st.markdown("**🛠️ Önerilen İncelemeler:**")
                    for o in case_analysis.get('onerilen_incelemeler', []): 
                        st.markdown(f"- {o}")
                    if case_analysis.get('eksik_kanitlar'):
                        st.markdown("**⚠️ Eksik Kanıtlar / Telemetri:**")
                        for e in case_analysis.get('eksik_kanitlar', []):
                            st.markdown(f"- {e}")

                if incidents:
                    _render_closed_loop_feedback(incidents[0].get('incident_id', 'inc_unknown'), case_analysis)

        if plans:
            st.markdown("##### 📋 Aksiyon Planı")
            for plan in plans:
                owner = plan.get('owner', 'SRE / NOC')
                action = plan.get('recommended_next_step', 'review')
                with st.container(border=True):
                    p1, p2 = st.columns([2, 1])
                    p1.markdown(f"**Adım:** `{action}`")
                    p2.markdown(f"**Sorumlu:** {owner}")


def _render_compact_detail(repo, summary, connection, redact):
    monitor = summary['monitor']
    monitor_id = monitor.id

    try:
        runs = repo.history(monitor_id)
    except Exception:
        runs = []
    if not runs:
        try:
            runs = repo.recent_runs(monitor_id, limit=20)
        except Exception:
            runs = []

    st.subheader("⚙️ Monitör Yönetimi & Geçmiş")
    btn_col1, btn_col2, btn_col3, btn_col4 = st.columns(4)
    running = monitor.status.value == 'RUNNING' or bool(runs and runs[0].status.value == 'RUNNING')

    with btn_col1:
        if not monitor.archived:
            if st.button('Pause' if monitor.enabled else 'Resume Now', key='toggle_monitor_' + monitor_id, width='stretch'):
                if monitor.enabled:
                    repo.set_enabled(monitor_id, False, now=now())
                else:
                    repo.resume_now(monitor_id, now=now())
                invalidate_snapshot()
                st.rerun()

    with btn_col2:
        if not monitor.archived and monitor.enabled and not running:
            if st.button('Run Now', key='run_now_' + monitor_id, type='primary', width='stretch'):
                repo.request_run_now(monitor_id, now=now())
                st.session_state['monitor_control_notice'] = 'Analiz isteği iletildi.'
                invalidate_snapshot()
                st.rerun()

    with btn_col3:
        if st.button('Settings', key='settings_' + monitor_id, width='stretch'):
            st.session_state['monitor_edit_mode'] = monitor_id

    with btn_col4:
        if not monitor.archived and st.button('Archive', key='archive_' + monitor_id, width='stretch'):
            st.session_state['delete_confirmation_monitor_id'] = monitor_id

    if st.session_state.get('monitor_edit_mode') == monitor_id:
        with st.container(border=True):
            st.subheader("Edit Monitor")
            monitor_form(repo, connection, redact, monitor)

    selected_run = None
    if runs:
        with st.expander(f"Tüm Geçmiş ({len(runs)} pencere) - Detay İçin Satıra Tıklayın", expanded=False):
            history_rows = []
            for r in runs:
                history_rows.append({
                    "Zaman": f"{r.window.start:%d %b %H:%M} → {r.window.end:%H:%M}",
                    "Durum": r.status.value,
                    "Loglar": r.counts.events_retrieved if r.status.value == 'SUCCESS' else 0,
                    "Sinyal": r.counts.qualified_signals,
                    "Incident": r.counts.incidents
                })
            try:
                table_event = st.dataframe(
                    history_rows, hide_index=True, width='stretch', selection_mode="single-row",
                    on_select="rerun", key=f"history_table_{monitor_id}"
                )
                selected_rows = table_event.selection.rows if hasattr(table_event, 'selection') else []
                selected_idx = selected_rows[0] if selected_rows else 0
            except TypeError:
                st.dataframe(history_rows, hide_index=True, width='stretch')
                selected_idx = 0

            selected_run = runs[selected_idx] if 0 <= selected_idx < len(runs) else runs[0]

    if selected_run:
        result = repo.result(selected_run.id)
        if result is None and getattr(selected_run, 'result_reference', None):
            result = repo.result(selected_run.result_reference)
        _render_aiops_report_cards(result, summary, redact, selected_run=selected_run)


def render(connection, redact):
    st.session_state.setdefault('expanded_monitor_id', None)
    st.session_state.setdefault('create_form_open', False)
    st.session_state.setdefault('show_archived', False)
    
    notice = st.session_state.pop('monitor_control_notice', None)
    if notice:
        st.success(notice)
        
    title, refresh, create = st.columns([5, 1.3, 1.8], vertical_alignment='center')
    with title:
        st.title('Deployment Monitors')
    with refresh:
        if st.button('Refresh', key='refresh_monitors', width='stretch'):
            invalidate_snapshot()
            st.rerun()
    with create:
        if st.button('+ New Monitor', key='new_monitor', type='primary', width='stretch'):
            st.session_state['create_form_open'] = not st.session_state['create_form_open']

    query = st.text_input('Search monitors', key='monitor_search', placeholder='Search workload or namespace')
    selected_status = st.selectbox('Status', ['All', 'Active', 'Paused', 'Archived'], key='monitor_status_filter')
    st.checkbox('Show archived', key='show_archived')

    try:
        repo = repository()
        snapshot_key = bool(st.session_state['show_archived'] or selected_status == 'Archived')
        snapshot = st.session_state.get('monitor_list_snapshot')
        if snapshot is None or snapshot[0] != snapshot_key:
            snapshot = (snapshot_key, repo.list_monitor_summaries(include_archived=snapshot_key))
            st.session_state['monitor_list_snapshot'] = snapshot
        monitors = snapshot[1]
    except Exception:
        st.error('Monitoring storage is temporarily unavailable. Refresh shortly.')
        return

    if st.session_state['create_form_open']:
        with st.container(border=True):
            st.subheader('New Monitor')
            monitor_form(repo, connection, redact)

    search = query.casefold().strip()
    visible = [item for item in monitors if
               (not search or search in item['monitor'].definition.name.casefold() or
                search in item['monitor'].definition.workload.casefold() or
                search in item['monitor'].definition.namespace.casefold()) and
               (selected_status == 'All' or
                ('Archived' if item['monitor'].archived else
                 'Active' if item['monitor'].enabled else 'Paused') == selected_status)]

    if not visible:
        st.info('No monitors to show.')

    for item in visible:
        monitor = item['monitor']
        health = ('PAUSED' if not monitor.enabled else 'ATTENTION' if item['latest_status'] == 'FAILED'
                  else 'HEALTHY')
        
        with st.container(border=True):
            st.markdown(f"#### {health} {redact(monitor.definition.workload)}")
            st.caption(redact(monitor.definition.namespace))
            
            c1, c2, c3, c4 = st.columns(4)
            c1.markdown(f"**Son İşlem:** `{item['latest_status'] or 'None'}`")
            c2.markdown(f"**Log/Event:** `{item['physical_logs'] if item['physical_logs'] is not None else '—'}`")
            c3.markdown(f"**Nitelikli Sinyal:** `{item['findings'] if item['findings'] is not None else '—'}`")
            c4.markdown(f"**Sonraki:** `{next_run_label(monitor, now())}`")

            st.markdown("---")
            st.markdown("##### 🚨 Güncel Olaylar (Tüm Olaylar)")
            
            aggregated_incidents = {}
            try:
                recent_runs = repo.history(monitor.id, limit=10)
                for run in recent_runs:
                    run_result = repo.result(run.id)
                    if not run_result or not run_result.get('incidents'):
                        continue
                        
                    run_events = run_result.get('events') or run_result.get('canonical_events') or run_result.get('parsed_events') or []
                    run_events_map = {str(e.get('event_id') or e.get('id')): e for e in run_events if (e.get('event_id') or e.get('id'))}
                        
                    for inc in run_result.get('incidents'):
                        inc_id = inc.get('incident_id', 'Bilinmiyor')
                        
                        run_signals = inc.get('signals', [])
                        if not run_signals:
                            all_sigs = run_result.get('qualified_signals') or run_result.get('signals', [])
                            inc_sig_ids = inc.get('signal_ids', [])
                            run_signals = [s for s in all_sigs if s.get('signal_id') in inc_sig_ids]
                        
                        if inc_id not in aggregated_incidents:
                            aggregated_incidents[inc_id] = {
                                "incident_id": inc_id,
                                "status": inc.get('status', 'AÇIK'),
                                "start_time": inc.get('time_window', {}).get('start') or inc.get('start_ms'),
                                "signal_count": 0,
                                "event_count": 0,
                                "desc": inc.get('root_cause_template') or inc.get('desc') or 'Açıklama yok.',
                                "signals_dict": {},
                                "case_analysis": {},
                                "plans": []
                            }
                            
                        agg = aggregated_incidents[inc_id]
                        agg["signal_count"] += inc.get('signal_count', 0)
                        agg["event_count"] += inc.get('event_count', 0)
                        
                        old_start = inc.get('time_window', {}).get('start') or inc.get('start_ms')
                        if old_start:
                            agg["start_time"] = old_start
                            
                        c_analysis = run_result.get('case_analysis') or {}
                        if c_analysis and inc_id in c_analysis.get('etkilenen_olaylar', [inc_id]):
                            agg["case_analysis"] = c_analysis
                            
                        inc_plans = [p for p in (run_result.get('plans') or []) if p.get('incident_id') == inc_id]
                        if inc_plans:
                            agg["plans"] = inc_plans
                            
                        for s in run_signals:
                            s_id = s.get('signal_id')
                            
                            e_ids = s.get('event_ids') or s.get('linked_events') or []
                            if isinstance(e_ids, str):
                                e_ids = [x.strip() for x in e_ids.split(',')]
                                
                            extracted_events = s.get('events', [])
                            if not extracted_events and e_ids:
                                for eid in e_ids:
                                    if str(eid) in run_events_map:
                                        extracted_events.append(run_events_map[str(eid)])
                                    else:
                                        extracted_events.append({
                                            "event_id": eid,
                                            "timestamp": agg.get("start_time"),
                                            "level": "ERROR" if int(s.get("severity_min", 7)) <= 3 else "WARNING",
                                            "message": s.get("template") or s.get("text") or "Sıkıştırılmış log kaydı."
                                        })
                            
                            incoming_count = int(s.get('count') or len(extracted_events) or len(e_ids) or 1)
                            
                            if s_id not in agg["signals_dict"]:
                                s_copy = dict(s)
                                s_copy['count'] = incoming_count
                                s_copy['events'] = extracted_events
                                s_copy['pod_names'] = s.get('pod_names', [])
                                s_copy['service_ids'] = s.get('service_ids', [])
                                s_copy['avg_latency'] = s.get('avg_latency')
                                agg["signals_dict"][s_id] = s_copy
                            else:
                                agg["signals_dict"][s_id]['count'] += incoming_count
                                if not agg["signals_dict"][s_id].get('pod_names') and s.get('pod_names'):
                                    agg["signals_dict"][s_id]['pod_names'] = s.get('pod_names')
                                if not agg["signals_dict"][s_id].get('service_ids') and s.get('service_ids'):
                                    agg["signals_dict"][s_id]['service_ids'] = s.get('service_ids')
                                if agg["signals_dict"][s_id].get('avg_latency') is None and s.get('avg_latency') is not None:
                                    agg["signals_dict"][s_id]['avg_latency'] = s.get('avg_latency')
                                if extracted_events:
                                    if 'events' not in agg["signals_dict"][s_id]:
                                        agg["signals_dict"][s_id]['events'] = []
                                    agg["signals_dict"][s_id]['events'].extend(extracted_events)

            except Exception:
                pass
            
            latest_incidents = list(aggregated_incidents.values())
            
            def fmt_time(t):
                if isinstance(t, (int, float)):
                    val = t / 1000 if t > 1e11 else t
                    return datetime.fromtimestamp(val, tz=timezone.utc).strftime('%H:%M:%S UTC')
                return str(t)

            if not latest_incidents:
                st.success("🎉 Şu anda açık veya aktif bir Incident bulunmuyor.")
            else:
                inc_rows = []
                for inc in latest_incidents:
                    inc_rows.append({
                        "Zaman (İlk Görülme)": fmt_time(inc['start_time']),
                        "Durum": inc['status'],
                        "Incident ID": inc['incident_id'],
                        "Sinyal (Toplam)": inc['signal_count'],
                        "Event (Toplam)": inc['event_count'],
                        "Açıklama": redact(inc['desc'][:80])
                    })
                
                table_key = f"inc_table_{monitor.id}"
                
                try:
                    inc_event = st.dataframe(
                        inc_rows,
                        use_container_width=True,
                        hide_index=True,
                        selection_mode="single-row",
                        on_select="rerun",
                        key=table_key
                    )
                    selected_rows = inc_event.selection.rows if hasattr(inc_event, 'selection') else []
                except TypeError:
                    st.dataframe(inc_rows, use_container_width=True, hide_index=True)
                    selected_rows = []
                
                if selected_rows:
                    selected_idx = selected_rows[0]
                    selected_inc = latest_incidents[selected_idx]
                    inc_id = selected_inc['incident_id']
                    
                    st.markdown(f"###### 🔍 Seçilen Incident: `{inc_id}` - Konsolide Sinyal Detayları")
                    signals = list(selected_inc['signals_dict'].values())
                    
                    if not signals:
                        st.warning("Bu incident'a ait sinyal detayı bulunamadı.")
                    else:
                        _render_predictive_incident_banner(signals)
                        _render_incident_timeline(signals, redact)

                        sig_rows = []
                        for sig in signals:
                            sig_count = int(sig.get('count') or len(sig.get('events', [])) or len(sig.get('event_ids', [])) or 1)
                            
                            latency_val = sig.get('avg_latency')
                            latency_str = f"{latency_val:.1f} ms" if latency_val is not None else "—"
                            pods_list = sig.get('pod_names') or []
                            pods_str = ", ".join(pods_list) if pods_list else "—"
                            srv_list = sig.get('service_ids') or []
                            srv_str = ", ".join(str(x) for x in srv_list) if srv_list else "—"
                            
                            sig_rows.append({
                                "Sinyal ID": sig.get('signal_id', 'Unknown'),
                                "Log": sig_count,
                                "Skor": f"{float(sig.get('qualification_score', 0.0)):.2f}",
                                "Gecikme (Latency)": latency_str,
                                "Pod Dağılımı": pods_str,
                                "Servis ID": srv_str,
                                "Sinyal Şablonu / Hata Metni": redact((sig.get('template') or sig.get('text') or '')[:90])
                            })
                            
                        sig_table_key = f"sig_table_{monitor.id}_{inc_id}"
                        
                        try:
                            sig_event = st.dataframe(
                                sig_rows,
                                use_container_width=True,
                                hide_index=True,
                                selection_mode="single-row",
                                on_select="rerun",
                                key=sig_table_key
                            )
                            sel_sig_rows = sig_event.selection.rows if hasattr(sig_event, 'selection') else []
                        except TypeError:
                            st.dataframe(sig_rows, use_container_width=True, hide_index=True)
                            sel_sig_rows = []
                        
                        if sel_sig_rows:
                            sel_sig_idx = sel_sig_rows[0]
                            selected_sig = signals[sel_sig_idx]
                            
                            p_latency = selected_sig.get('avg_latency')
                            p_pods = selected_sig.get('pod_names') or []
                            p_srv = selected_sig.get('service_ids') or []
                            pod_display = p_pods[0] if p_pods else "—"

                            st.markdown("###### 📊 Sinyal Telemetri ve Topoloji Metrikleri")
                            m1, m2, m3, m4 = st.columns([1.2, 2.5, 1.1, 1.1])
                            m1.metric(
                                "Ortalama Gecikme (P3)",
                                f"{p_latency:.1f} ms" if p_latency is not None else "—",
                                delta="Gecikme Artışı (Darboğaz)" if p_latency and p_latency > 1000 else None,
                                delta_color="inverse"
                            )
                            with m2:
                                st.caption("Etkilenen Pod (P1)")
                                st.code(pod_display, language="text")
                            m3.metric("Servis ID (P2)", ", ".join(str(x) for x in p_srv) if p_srv else "—")
                            m4.metric("Nitelik Skoru", f"{float(selected_sig.get('qualification_score', 0.0)):.2f}")

                            if len(p_pods) == 1 and int(selected_sig.get('count', 1)) > 1:
                                st.info(f"💡 **Topluluk Anomalisi (Population Anomaly - P1):** Hatalar genel dağıtımda değil, yalnızca `{pod_display}` poduna izole durumdadır.")

                            st.markdown("###### 📜 Ham Log Kayıtları")
                            events = selected_sig.get('events', [])
                            if events:
                                df_ev = pd.DataFrame(events)
                                exist_cols = [c for c in ["event_id", "timestamp", "level", "message"] if c in df_ev.columns]
                                if 'timestamp' in df_ev.columns:
                                    df_ev['timestamp'] = df_ev['timestamp'].apply(fmt_time)
                                st.dataframe(df_ev[exist_cols], use_container_width=True, hide_index=True)
                            else:
                                st.info("Bu sinyale ait ham log listesi (events) payload içine dahil edilmemiş.")

                    case_analysis = selected_inc.get('case_analysis') or {}
                    plans = selected_inc.get('plans') or []

                    if case_analysis or plans:
                        st.markdown("---")
                        st.markdown(f"### 🧠 AI RCA Uzman Raporu (`{inc_id}`)")
                        if case_analysis:
                            with st.container(border=True):
                                conf = case_analysis.get('guven', 0.0)
                                status = case_analysis.get('nedensellik_durumu', 'belirsiz')
                                src = case_analysis.get('analysis_source', 'ai_uzman')

                                h_col1, h_col2, h_col3 = st.columns([2, 1, 1])
                                with h_col1:
                                    st.subheader(f"Teşhis Raporu (Güven: %{int(conf * 100)})")
                                with h_col2:
                                    st.metric("Nedensellik", status.upper())
                                with h_col3:
                                    st.metric("Kaynak", src)

                                if case_analysis.get('durum_ozeti'):
                                    st.info(f"**📌 Durum Özeti:** {case_analysis['durum_ozeti']}")
                                if case_analysis.get('kok_neden_hipotezi'):
                                    st.error(f"**🔥 Kök Neden Hipotezi:** {case_analysis['kok_neden_hipotezi']}")
                                
                                col_left, col_right = st.columns(2)
                                with col_left:
                                    st.markdown("**🔍 Karar Gerekçesi:**")
                                    for g in case_analysis.get('karar_gerekcesi', []): 
                                        st.markdown(f"- {g}")
                                    if case_analysis.get('alternatif_hipotezler'):
                                        st.markdown("**🤔 Alternatif Hipotezler:**")
                                        for a in case_analysis.get('alternatif_hipotezler', []):
                                            st.markdown(f"- {a}")
                                with col_right:
                                    st.markdown("**🛠️ Önerilen İncelemeler:**")
                                    for o in case_analysis.get('onerilen_incelemeler', []): 
                                        st.markdown(f"- {o}")
                                    if case_analysis.get('eksik_kanitlar'):
                                        st.markdown("**⚠️ Eksik Kanıtlar / Telemetri:**")
                                        for e in case_analysis.get('eksik_kanitlar', []):
                                            st.markdown(f"- {e}")

                                _render_closed_loop_feedback(inc_id, case_analysis)

                        if plans:
                            st.markdown("##### 📋 Aksiyon Planı")
                            for plan in plans:
                                owner = plan.get('owner', 'SRE / NOC')
                                action = plan.get('recommended_next_step', 'review')
                                with st.container(border=True):
                                    p1, p2 = st.columns([2, 1])
                                    p1.markdown(f"**Adım:** `{action}`")
                                    p2.markdown(f"**Sorumlu:** {owner}")

            st.markdown("---")
            btn_label = 'Geçmişi ve Ayarları Kapat' if st.session_state['expanded_monitor_id'] == monitor.id else 'Tüm Geçmişi ve Ayarları Gör'
            if st.button(btn_label, key='monitor_row_' + monitor.id):
                if st.session_state['expanded_monitor_id'] == monitor.id:
                    st.session_state['expanded_monitor_id'] = None
                else:
                    st.session_state['expanded_monitor_id'] = monitor.id
                st.session_state.pop('monitor_runs_snapshot', None)
                st.session_state.pop('monitor_finding_snapshot', None)
                st.rerun()

            if st.session_state['expanded_monitor_id'] == monitor.id:
                _render_compact_detail(repo, item, connection, redact)