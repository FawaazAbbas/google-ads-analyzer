"""
Google Ads Analyzer — Streamlit Web UI
Run with: streamlit run app.py
"""
import os
from pathlib import Path
from datetime import datetime

import tempfile

import streamlit as st
from dotenv import load_dotenv

# Load .env so ANTHROPIC_API_KEY is available if present
load_dotenv()

from agent.runner import run_analysis, EXPECTED_FILES, FILE_LABELS
from tools.utils import classify_csv, TYPE_TO_FILENAME, TYPE_LABELS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

TOOL_LABELS = {
    "analyze_campaign_performance":   "Campaign Performance",
    "analyze_budget_pacing":          "Budget Pacing",
    "analyze_keywords":               "Keywords & Quality Score",
    "analyze_search_terms":           "Search Terms",
    "analyze_ad_creatives":           "Ad Creatives",
    "analyze_ad_group_structure":     "Ad Group Structure",
    "analyze_bidding_strategies":     "Bidding Strategies",
    "analyze_audiences":              "Audiences",
    "analyze_devices":                "Device Performance",
    "analyze_time_performance":       "Time of Day / Day of Week",
    "analyze_extensions":             "Ad Extensions",
    "analyze_geographic_performance": "Geographic Performance",
}

# ---------------------------------------------------------------------------
# Page config — must be the first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Google Ads Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Custom CSS for a cleaner look
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .main-header { font-size: 2.2rem; font-weight: 700; margin-bottom: 0; }
    .sub-header  { color: #6b7280; margin-top: 0; margin-bottom: 1.5rem; }
    .file-row    { padding: 6px 0; border-bottom: 1px solid #f0f0f0; }
    .step-label  { font-weight: 600; font-size: 0.85rem; color: #374151;
                   text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
    .log-box     { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px;
                   padding: 1rem 1.2rem; font-family: monospace; font-size: 0.9rem;
                   line-height: 1.8; min-height: 80px; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------
defaults = {
    "analysis_running":  False,
    "analysis_complete": False,
    "report_text":       "",
    "log_lines":         [],
    "error_message":     "",
}
for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def classify_uploaded_file(uf):
    """
    Classify an UploadedFile by writing it to a temp file and inspecting its headers.
    Returns a type key string (e.g. "campaigns") or None.
    """
    # Fast path: if the filename exactly matches a known canonical name, trust it
    canonical_names = set(EXPECTED_FILES)
    if uf.name in canonical_names:
        # Reverse-lookup: filename → type key
        for k, v in TYPE_TO_FILENAME.items():
            if v == uf.name:
                return k

    # Content-based classification via temp file
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(uf.getvalue())
            tmp_path = tmp.name
        detected = classify_csv(tmp_path)
    except Exception:
        detected = None
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
    return detected


def save_uploaded_files(uploaded_files):
    """
    Classify each uploaded CSV by content, save to canonical filename in data/.
    Returns the list of canonical filenames that were saved successfully.
    """
    DATA_DIR.mkdir(exist_ok=True)
    saved = []
    for uf in uploaded_files:
        file_type = classify_uploaded_file(uf)
        if file_type:
            canonical = TYPE_TO_FILENAME[file_type]
            (DATA_DIR / canonical).write_bytes(uf.getvalue())
            saved.append(canonical)
    return saved


def get_api_key(manual_key=""):
    env_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    return env_key if env_key else manual_key.strip()


def do_rerun():
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()


def reset_state():
    for key, val in defaults.items():
        st.session_state[key] = val


def render_log(placeholder, lines, current=""):
    """Re-render the progress log inside a placeholder."""
    parts = []
    for line in lines:
        parts.append(line)
    if current:
        parts.append("⏳ " + current + "…")
    placeholder.markdown(
        '<div class="log-box">' + "<br>".join(parts) + "</div>",
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown('<p class="main-header">📊 Google Ads Analyzer</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="sub-header">Upload your Google Ads CSV exports and get an AI-powered '
    'optimization report in minutes.</p>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Step 1 — API Key
# ---------------------------------------------------------------------------
env_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
manual_api_key = ""

st.markdown('<p class="step-label">Step 1 — API Key</p>', unsafe_allow_html=True)

if env_key:
    st.success("✓ Anthropic API key loaded from your environment.")
else:
    manual_api_key = st.text_input(
        "Anthropic API Key",
        type="password",
        placeholder="sk-ant-...",
        help="Your key is used only for this session and is never stored.",
        disabled=st.session_state.analysis_running,
    )
    if not manual_api_key:
        st.caption("Get your API key at console.anthropic.com → API Keys")

st.divider()

# ---------------------------------------------------------------------------
# Step 2 — Upload CSV files
# ---------------------------------------------------------------------------
st.markdown('<p class="step-label">Step 2 — Upload Google Ads CSV Exports</p>', unsafe_allow_html=True)
st.caption(
    "Export reports from **Google Ads → Reports** and upload them here. "
    "You don't need all files — the agent skips any that are missing."
)

uploaded_files = st.file_uploader(
    "Drag & drop CSV files here",
    type=["csv"],
    accept_multiple_files=True,
    disabled=st.session_state.analysis_running,
    label_visibility="collapsed",
)

recognised = []   # canonical filenames that will be analysed

if uploaded_files:
    col1, col2 = st.columns(2)
    entries = []
    for uf in uploaded_files:
        detected = classify_uploaded_file(uf)
        entries.append((uf.name, detected))
        if detected:
            recognised.append(TYPE_TO_FILENAME[detected])

    mid = (len(entries) + 1) // 2
    for i, (fname, detected) in enumerate(entries):
        col = col1 if i < mid else col2
        if detected:
            label = TYPE_LABELS.get(detected, detected)
            col.markdown("✅ **{}**  \n*Detected: {}*".format(fname, label))
        else:
            col.markdown("⚠️ **{}**  \n*Could not identify — will skip*".format(fname))
else:
    # Show a quick cheat sheet of what to export
    with st.expander("What files should I export? (click to expand)"):
        for type_key, label in TYPE_LABELS.items():
            st.markdown("- **{}** — {}".format(TYPE_TO_FILENAME[type_key], label))

st.divider()

# ---------------------------------------------------------------------------
# Step 3 — Run button
# ---------------------------------------------------------------------------
st.markdown('<p class="step-label">Step 3 — Run Analysis</p>', unsafe_allow_html=True)

recognised = list(dict.fromkeys(recognised))  # deduplicate, preserve order
has_files  = bool(recognised)
has_key    = bool(get_api_key(manual_api_key))
can_run    = has_files and has_key and not st.session_state.analysis_running
is_done    = st.session_state.analysis_complete or bool(st.session_state.error_message)

btn_col, hint_col = st.columns([1, 3])

with btn_col:
    run_clicked = st.button(
        "🚀  Run Analysis",
        disabled=not can_run,
        type="primary",
        use_container_width=True,
    )

with hint_col:
    if not has_files:
        st.caption("Upload at least one CSV file to continue.")
    elif not has_key:
        st.caption("Enter your Anthropic API key to continue.")
    elif st.session_state.analysis_running:
        st.caption("Analysis in progress…")
    elif is_done:
        if st.button("↩  Start Over", use_container_width=False):
            reset_state()
            do_rerun()

# ---------------------------------------------------------------------------
# Run logic — executes when button is clicked
# ---------------------------------------------------------------------------
if run_clicked and can_run:
    saved = save_uploaded_files(uploaded_files)
    if not saved:
        st.error("No recognised CSV files could be saved. Check your filenames.")
        st.stop()

    api_key = get_api_key(manual_api_key)

    st.session_state.analysis_running  = True
    st.session_state.analysis_complete = False
    st.session_state.report_text       = ""
    st.session_state.log_lines         = []
    st.session_state.error_message     = ""

    st.divider()
    st.markdown('<p class="step-label">Analysis Progress</p>', unsafe_allow_html=True)
    progress_placeholder = st.empty()
    render_log(progress_placeholder, [], "Starting")

    log_lines = []

    for event_type, data in run_analysis(saved, api_key):

        if event_type == "status":
            log_lines.append("ℹ️ " + data)
            render_log(progress_placeholder, log_lines)

        elif event_type == "thinking":
            render_log(progress_placeholder, log_lines, "Claude is thinking")

        elif event_type == "tool_start":
            label = TOOL_LABELS.get(data, data)
            render_log(progress_placeholder, log_lines, "Analyzing " + label)

        elif event_type == "tool_done":
            label = TOOL_LABELS.get(data, data)
            log_lines.append("✅ " + label)
            render_log(progress_placeholder, log_lines)

        elif event_type == "complete":
            log_lines.append("🎉 Analysis complete!")
            render_log(progress_placeholder, log_lines)
            st.session_state.report_text       = data
            st.session_state.analysis_complete = True
            st.session_state.analysis_running  = False

        elif event_type == "error":
            st.session_state.error_message    = data
            st.session_state.analysis_running = False
            break

    if st.session_state.error_message:
        st.error("❌ " + st.session_state.error_message)

# ---------------------------------------------------------------------------
# Report display (persists across reruns via session_state)
# ---------------------------------------------------------------------------
if st.session_state.analysis_complete and st.session_state.report_text:
    st.divider()

    dl_col, spacer = st.columns([1, 3])
    with dl_col:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label="⬇️  Download Report (.md)",
            data=st.session_state.report_text.encode("utf-8"),
            file_name="google_ads_report_{}.md".format(ts),
            mime="text/markdown",
            type="primary",
            use_container_width=True,
        )

    st.divider()
    st.markdown(st.session_state.report_text)
