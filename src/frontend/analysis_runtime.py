"""Existing upload conversion and cached pipeline lifecycle, shared by UI pages."""
from __future__ import annotations

import csv
import io
import json
import os
from pathlib import Path
import tempfile
from threading import Lock

import streamlit as st
import full_pipeline_v2


@st.cache_resource
def get_pipeline():
    return full_pipeline_v2.FullAIOpsPipelineV2(use_ai_rca=True)


@st.cache_resource
def get_analysis_lock():
    # Learning persists; analyses, including uploads, have one owner at a time.
    return Lock()


ALARM_SEVERITY = {1: 'INFO', 2: 'WARNING', 3: 'ERROR', 4: 'ERROR', 5: 'CRITICAL'}


def _remove_upload(path):
    # Preserve the existing upload cleanup/error behavior.
    try:
        os.unlink(path)
    except OSError:
        pass


def _normalize_alarm_record(record):
    row = dict(record or {})
    if row.get('alarm_id') and row.get('alarm_type'):
        try:
            level = int(row.get('severity'))
            row['severity'] = ALARM_SEVERITY.get(level, row.get('severity'))
            row['source_severity'] = level
        except (TypeError, ValueError):
            pass
        if row.get('servis') and not row.get('service'):
            row['service'] = row.get('servis')
    return row


def _uploaded_to_pipeline_text(upload):
    text = upload.getvalue().decode('utf-8-sig', errors='replace')
    suffix = Path(upload.name).suffix.lower()
    if suffix == '.csv':
        rows = list(csv.DictReader(io.StringIO(text)))
        if not rows:
            raise ValueError('CSV dosyasında veri satırı bulunamadı.')
        return '\n'.join(json.dumps(_normalize_alarm_record(row), ensure_ascii=False) for row in rows) + '\n', '.log'
    if suffix in {'.json', '.txt'}:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            data = None
        if isinstance(data, list) and all(isinstance(row, dict) for row in data):
            return '\n'.join(json.dumps(_normalize_alarm_record(row), ensure_ascii=False) for row in data) + '\n', '.log'
        if isinstance(data, dict):
            return json.dumps(_normalize_alarm_record(data), ensure_ascii=False) + '\n', '.log'
    return text, suffix or '.log'


def run_uploaded(upload, *, source_timezone=None):
    if Path(upload.name).suffix.lower() == '.zip':
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as file:
            file.write(upload.getvalue())
            path = file.name
        try:
            return get_pipeline().process_package(path, source_timezone=source_timezone)
        finally:
            _remove_upload(path)
    content, suffix = _uploaded_to_pipeline_text(upload)
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode='w', encoding='utf-8', newline='') as file:
        file.write(content)
        path = file.name
    try:
        return get_pipeline().process_file(path, source_timezone=source_timezone)
    finally:
        _remove_upload(path)
