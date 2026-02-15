"""
Retail Insights — Streamlit App
================================
Run with:  streamlit run app.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── path so flat imports (state, models, graph, …) all resolve ───────────────
sys.path.insert(0, str(Path(__file__).parent))

from dataprocessing.datalayer import load_and_profile
from agents.query_resolution_agent import build_metadata_context, trim_history
from graph import build_graph
from state import RetailAgenticState
from agents.summarizer import generate_summary


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Retail Insights",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@300;400;500&display=swap');

/* ── base ── */
html, body, [class*="css"] {
    font-family: 'DM Mono', monospace;
    background: #0e0e0f;
    color: #e8e6e1;
}

/* ── top header bar ── */
.ri-header {
    display: flex;
    align-items: baseline;
    gap: 12px;
    padding: 2rem 0 1.2rem 0;
    border-bottom: 1px solid #2a2a2e;
    margin-bottom: 2rem;
}
.ri-header h1 {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 1.9rem;
    color: #f0ede6;
    margin: 0;
    letter-spacing: -0.5px;
}
.ri-header .badge {
    font-size: 0.72rem;
    font-weight: 500;
    background: #1e3a2f;
    color: #4ade80;
    border: 1px solid #166534;
    padding: 2px 10px;
    border-radius: 99px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}

/* ── upload zone ── */
.upload-hint {
    text-align: center;
    color: #6b6b72;
    font-size: 0.85rem;
    padding: 3rem 0 1rem 0;
}
.upload-hint .big {
    font-family: 'Syne', sans-serif;
    font-size: 2.2rem;
    font-weight: 700;
    color: #3a3a42;
    display: block;
    margin-bottom: 0.5rem;
}

/* ── stat cards ── */
.stat-row {
    display: flex;
    gap: 12px;
    margin-bottom: 1.5rem;
}
.stat-card {
    flex: 1;
    background: #16161a;
    border: 1px solid #2a2a2e;
    border-radius: 8px;
    padding: 1rem 1.2rem;
}
.stat-card .label {
    font-size: 0.72rem;
    color: #6b6b72;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 4px;
}
.stat-card .value {
    font-family: 'Syne', sans-serif;
    font-size: 1.5rem;
    font-weight: 700;
    color: #f0ede6;
}

/* ── chat bubbles ── */
.msg-user {
    display: flex;
    justify-content: flex-end;
    margin: 0.6rem 0;
}
.msg-user .bubble {
    background: #1a2e1e;
    border: 1px solid #166534;
    color: #dcfce7;
    padding: 0.65rem 1rem;
    border-radius: 16px 16px 4px 16px;
    max-width: 72%;
    font-size: 0.88rem;
    line-height: 1.5;
}
.msg-assistant {
    display: flex;
    justify-content: flex-start;
    margin: 0.6rem 0;
}
.msg-assistant .bubble {
    background: #16161a;
    border: 1px solid #2a2a2e;
    color: #e8e6e1;
    padding: 0.65rem 1rem;
    border-radius: 16px 16px 16px 4px;
    max-width: 80%;
    font-size: 0.88rem;
    line-height: 1.5;
}
.msg-assistant .label {
    font-size: 0.68rem;
    color: #6b6b72;
    margin-bottom: 4px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}

/* ── sql expander ── */
.sql-tag {
    font-size: 0.72rem;
    color: #6b6b72;
    cursor: pointer;
    margin-top: 4px;
    display: inline-block;
}

/* ── input row ── */
.stTextInput > div > div > input {
    background: #16161a !important;
    border: 1px solid #2a2a2e !important;
    color: #e8e6e1 !important;
    border-radius: 8px !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 0.88rem !important;
}
.stTextInput > div > div > input:focus {
    border-color: #4ade80 !important;
    box-shadow: 0 0 0 2px rgba(74,222,128,0.15) !important;
}

/* ── buttons ── */
.stButton > button {
    background: #1a2e1e !important;
    color: #4ade80 !important;
    border: 1px solid #166534 !important;
    border-radius: 8px !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 0.82rem !important;
    padding: 0.4rem 1.2rem !important;
    transition: background 0.15s;
}
.stButton > button:hover {
    background: #14532d !important;
}

/* ── file uploader ── */
[data-testid="stFileUploader"] {
    background: #16161a;
    border: 1.5px dashed #2a2a2e;
    border-radius: 10px;
    padding: 1rem;
}

/* ── summary markdown ── */
.summary-wrap {
    background: #16161a;
    border: 1px solid #2a2a2e;
    border-radius: 10px;
    padding: 1.5rem 2rem;
    max-height: 520px;
    overflow-y: auto;
}

/* ── divider ── */
hr { border-color: #2a2a2e; margin: 1.5rem 0; }

/* ── streamlit chrome overrides ── */
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stSidebar"] { display: none; }
.block-container { padding: 1.5rem 3rem !important; max-width: 1100px; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown("""
<div class="ri-header">
    <h1>Retail Insights</h1>
    <span class="badge">AI-powered</span>
</div>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)


def _reset_session():
    # Close existing DuckDB connection
    if st.session_state.get("db_con") is not None:
        try:
            st.session_state["db_con"].close()
        except Exception:
            pass

    # Delete the previous CSV from the data folder
    prev_csv = st.session_state.get("csv_path")
    if prev_csv:
        try:
            Path(prev_csv).unlink(missing_ok=True)
        except Exception:
            pass

    # Wipe all session keys back to defaults
    for key in ["table_profile", "db_con", "metadata_str",
                 "graph", "summary_md", "chat_history", "file_id", "csv_path"]:
        st.session_state[key] = None
    st.session_state["chat_history"] = []


for key, default in [
    ("table_profile", None),
    ("db_con",        None),
    ("metadata_str",  None),
    ("graph",         None),
    ("summary_md",    None),
    ("chat_history",  []),
    ("file_id",       None),
    ("csv_path",     None),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ---------------------------------------------------------------------------
# File upload + session bootstrap
# ---------------------------------------------------------------------------

SUMMARY_TRIGGER = "summary"   # user types this exact word to get the cached summary

uploaded = st.file_uploader(
    "Upload a sales CSV to begin",
    type=["csv"],
    label_visibility="collapsed",
)

if uploaded is not None:
    file_id = f"{uploaded.name}_{uploaded.size}"

    # New file → reset everything and rerun the bootstrap
    if file_id != st.session_state.file_id:
        _reset_session()
        st.session_state.file_id = file_id

        # ── Phase 1: save file and profile ───────────────────────────────────
        with st.spinner("Loading your data …"):
            # Save into data/ with the original filename (overwrites any previous file)
            # Always save under a fixed name — no spaces, no path issues
            csv_path = DATA_DIR / "upload.csv"
            csv_path.write_bytes(uploaded.getvalue())
            st.session_state.csv_path = str(csv_path)

            profile = load_and_profile(csv_path)
            st.session_state.table_profile = profile
            st.session_state.metadata_str  = build_metadata_context(profile)

            # Open one persistent DuckDB connection for the session
            db_con = duckdb.connect(database=":memory:")
            db_con.execute(
                f"CREATE TABLE {profile.table_name} AS "
                f"SELECT * FROM read_csv_auto('{csv_path.resolve()}', header=true, ignore_errors=true, sample_size=-1)"
            )
            st.session_state.db_con = db_con

        # ── Phase 2: summarization (blocking — user sees spinner) ────────────
        with st.spinner("Analysing your data and building summary …"):
            try:
                summary = generate_summary(
                    st.session_state.metadata_str,
                    profile.table_name,
                    db_con,
                )
            except Exception as e:
                summary = f"Summary generation failed: {e}"
            st.session_state.summary_md = summary

        # ── Phase 3: compile graph ───────────────────────────────────────────
        st.session_state.graph = build_graph()
        st.rerun()


# ---------------------------------------------------------------------------
# Main UI — only rendered once a file is loaded and summarized
# ---------------------------------------------------------------------------

if st.session_state.table_profile is None:
    st.markdown("""
    <div class="upload-hint">
        <span class="big">↑</span>
        Upload a CSV file above to start analysing your sales data.
    </div>
    """, unsafe_allow_html=True)
    st.stop()

profile      = st.session_state.table_profile
db_con       = st.session_state.db_con
metadata_str = st.session_state.metadata_str
graph        = st.session_state.graph

# ── Dataset stat cards ────────────────────────────────────────────────────────
numeric_cols = sum(1 for c in profile.columns
                   if c.dtype.upper().split("(")[0].strip() in {
                       "TINYINT","SMALLINT","INTEGER","INT","BIGINT",
                       "FLOAT","DOUBLE","DECIMAL","REAL"
                   })

st.markdown(f"""
<div class="stat-row">
  <div class="stat-card">
    <div class="label">Table</div>
    <div class="value">{profile.table_name}</div>
  </div>
  <div class="stat-card">
    <div class="label">Rows</div>
    <div class="value">{profile.total_rows:,}</div>
  </div>
  <div class="stat-card">
    <div class="label">Columns</div>
    <div class="value">{profile.total_columns}</div>
  </div>
  <div class="stat-card">
    <div class="label">Numeric cols</div>
    <div class="value">{numeric_cols}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Hint strip ────────────────────────────────────────────────────────────────
st.info(
    f'💡 Type **`{SUMMARY_TRIGGER}`** to see the full auto-generated data summary, '
    'or ask any analytical question about your data.',
    icon=None,
)

st.markdown("<hr>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Chat rendering
# ---------------------------------------------------------------------------

chat_container = st.container()

with chat_container:
    for msg in st.session_state.chat_history:
        if msg["role"] == "user":
            st.markdown(
                f'<div class="msg-user"><div class="bubble">{msg["content"]}</div></div>',
                unsafe_allow_html=True,
            )
        else:
            content = msg.get("content", "")
            sql     = msg.get("sql", "")
            st.markdown(
                f'<div class="msg-assistant">'
                f'<div class="bubble">'
                f'<div class="label">Retail Insights</div>'
                f'{content}'
                f'</div></div>',
                unsafe_allow_html=True,
            )
            if sql and os.getenv("DEBUG"):
                with st.expander("SQL", expanded=False):
                    st.code(sql, language="sql")


# ---------------------------------------------------------------------------
# Input row
# ---------------------------------------------------------------------------

col_input, col_btn = st.columns([9, 1])

with col_input:
    user_input = st.text_input(
        "message",
        placeholder="Ask a question about your data …",
        label_visibility="collapsed",
        key="chat_input",
    )

with col_btn:
    send = st.button("Send", use_container_width=True)


# ---------------------------------------------------------------------------
# Message handling
# ---------------------------------------------------------------------------

if send and user_input.strip():
    query = user_input.strip()

    # Append user message to history
    st.session_state.chat_history.append({"role": "user", "content": query})

    # ── Summary shortcut ──────────────────────────────────────────────────────
    if query.lower().strip() == SUMMARY_TRIGGER:
        st.session_state.chat_history.append({
            "role":       "assistant",
            "content":    st.session_state.summary_md,
            "query_spec": None,
            "sql":        "",
        })
        st.session_state.chat_history = trim_history(st.session_state.chat_history)
        st.rerun()

    # ── Agentic pipeline ──────────────────────────────────────────────────────
    with st.spinner("Thinking …"):
        initial_state: RetailAgenticState = {
            "table_metadata":         metadata_str,
            "db_con":                 db_con,
            "table_name":             profile.table_name,
            "user_query":             query,
            "chat_history":           list(st.session_state.chat_history),
            "resolution":             None,
            "sql":                    "",
            "rows":                   [],
            "columns":                [],
            "validation_passed":      False,
            "validation_reason":      "",
            "validation_feedback":    "",
            "route_to":               "",
            "resolution_retry_count": 0,
            "extraction_retry_count": 0,
            "final_answer":           "",
            "messages":               [],
            "error":                  None,
        }

        try:
            final_state: RetailAgenticState = graph.invoke(initial_state)
            answer  = final_state.get("final_answer") or "No answer was produced."
            sql_out = final_state.get("sql", "")
            resolution = final_state.get("resolution")
            query_spec = resolution.model_dump() if resolution else None
        except Exception as exc:
            answer     = f"Something went wrong: {exc}"
            sql_out    = ""
            query_spec = None

    st.session_state.chat_history.append({
        "role":       "assistant",
        "content":    answer,
        "query_spec": query_spec,
        "sql":        sql_out,
    })
    st.session_state.chat_history = trim_history(st.session_state.chat_history)
    st.rerun()
