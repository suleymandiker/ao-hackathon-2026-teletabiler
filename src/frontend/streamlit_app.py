"""Deployment Monitors control plane. Execution belongs to monitor_worker."""

from pathlib import Path
import sys

import streamlit as st


ROOT = Path(__file__).resolve().parents[2]
for directory in (ROOT / 'src' / 'backend', Path(__file__).resolve().parent):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

import opensearch_application as openshift
from application_environment import load_environment
import monitoring_views
import theme


load_environment()
st.set_page_config(page_title='Deployment Monitors', page_icon='🔎', layout='wide')
st.markdown(theme.CSS, unsafe_allow_html=True)

logo = Path(__file__).resolve().parent / 'assets' / 'ai-in-ai-logo.png'
with st.sidebar:
    st.image(str(logo), width=161)
    st.caption('Deployment Monitors')

try:
    connection = openshift.load_connection()
except openshift.ApplicationError:
    connection = None

monitoring_views.render(connection, lambda value: openshift.safe_text(value, connection))
