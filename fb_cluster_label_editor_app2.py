# fb_cluster_explorer_editor_FAST.py
# Full code with performance fixes ONLY (no feature changes):
# - Loads the cluster sheet using a limited A1 range (faster than get_all_values)
# - Caches the PostID->row index for the "Facebook" sheet (so Apply is fast)
# - Keeps every other function/feature the same

import os
import time
import random
import re
from typing import Tuple

import pandas as pd
import streamlit as st
import plotly.express as px

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials


# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Facebook Cluster Explorer", layout="wide")

DEFAULT_SHEET_ID = "1wKxRXnaTzWxk1UwggJW0FTxbXHZlh6Njs_OwL55CxyU"
DEFAULT_CLUSTER_TAB = "Facebook_Grouped_Narratives"
TARGET_TAB = "Facebook"  # write corrections here

# Canonical column names we will use internally
COL_POSTID = "PostID"
COL_AUTHOR = "Author"
COL_AUTHORID = "AuthorId"
COL_CONTENT = "Content"
COL_URL = "Url"
COL_POSTED = "Posted At"
COL_NARR = "Narrative"
COL_MH = "Misinformation/Hate"
COL_ENG = "Total Engagement"
COL_CLEAN = "Clean_Content"
COL_CLUSTER = "Cluster_ID"
COL_REP = "Cluster_Representative"
COL_SIZE = "Cluster_Size"

NARRATIVE_OPTIONS = [
    "Neutral",
    "Discriminatory",
    "Ridiculing",
    "Seeding distrust",
    "Fearmongering",
    "Sweeping generalisations",
    "Slurs",
    "Othering",
    "Criminalizing",
    "Positive",
    "Call for harm",
    "Sterotyping",
    "Dehumanizing",
]

MH_OPTIONS = ["Hate Speech", "Misinformation", "N/A"]


# =========================
# AUTH
# =========================
def get_creds():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Streamlit secrets: file path
    if "SERVICE_ACCOUNT_FILE" in st.secrets:
        return Credentials.from_service_account_file(
            st.secrets["SERVICE_ACCOUNT_FILE"], scopes=scopes
        )

    # Streamlit secrets: inline json
    if "gcp_service_account" in st.secrets:
        return Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]), scopes=scopes
        )

    # Local
    if os.path.exists("credentials.json"):
        return Credentials.from_service_account_file("credentials.json", scopes=scopes)

    raise RuntimeError(
        "No credentials found. Put credentials.json next to this app, "
        "or configure Streamlit secrets."
    )


def open_sheet(sheet_id: str):
    creds = get_creds()
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)


def _is_transient(e: Exception) -> bool:
    msg = str(e)
    tokens = [
        "[500]", "[502]", "[503]", "[504]", "[429]",
        "Internal error", "backendError", "Server Error",
        "rateLimitExceeded", "Connection aborted", "WinError 10054",
        "ConnectionResetError", "ProtocolError"
    ]
    return any(t in msg for t in tokens)


def _sleep_backoff(attempt: int):
    base = 2 ** min(attempt, 6)
    jitter = random.uniform(0.0, 0.9)
    time.sleep(base + jitter)


def safe_batch_update(ws, updates: list, max_retries: int = 10):
    for attempt in range(max_retries):
        try:
            ws.batch_update(updates)
            return
        except Exception as e:
            if _is_transient(e):
                _sleep_backoff(attempt)
                continue
            raise
    raise RuntimeError("batch_update kept failing after retries.")


# =========================
# LOAD DATA (FAST VERSION)
# =========================
def _a1_cols(n: int) -> str:
    """Convert 1-based column number to Excel column letters."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


@st.cache_data(ttl=1800, show_spinner=False)
def detect_last_col_letter(sheet_id: str, tab_name: str) -> str:
    """
    Reads only the header row and detects the last used column.
    This avoids get_all_values().
    """
    sh = open_sheet(sheet_id)
    ws = sh.worksheet(tab_name)
    header = ws.row_values(1)
    header = [h.strip() for h in header if str(h).strip() != ""]
    if not header:
        return "A"
    return _a1_cols(len(header))


@st.cache_data(ttl=1800, show_spinner=False)
def load_tab_df_range(sheet_id: str, tab_name: str, end_col_letter: str) -> pd.DataFrame:
    """
    Fast load using ws.get() on a limited A1 range instead of get_all_values().
    Reads A1:<end_col> for all rows with values.
    """
    sh = open_sheet(sheet_id)
    ws = sh.worksheet(tab_name)
    a1_range = f"A1:{end_col_letter}"
    values = ws.get(a1_range)
    if not values or len(values) < 2:
        return pd.DataFrame()

    header = [str(h).strip() for h in values[0]]
    rows = values[1:]

    # Pad rows to header length
    n = len(header)
    fixed = []
    for r in rows:
        if len(r) < n:
            fixed.append(r + [""] * (n - len(r)))
        else:
            fixed.append(r[:n])

    return pd.DataFrame(fixed, columns=header)


def _norm_key(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", "", s)
    s = s.replace("_", "").replace("-", "")
    return s


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make column names consistent even if the sheet has:
    - trailing spaces
    - different casing
    - slightly different names (Cluster ID, ClusterID, etc.)
    """
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()
    d.columns = [str(c).strip() for c in d.columns]

    # Build mapping from normalized header -> actual header
    colmap = {_norm_key(c): c for c in d.columns}

    # Candidate keys for each canonical column
    want = {
        COL_POSTID: ["postid", "post_id", "post"],
        COL_AUTHOR: ["author", "username", "name"],
        COL_AUTHORID: ["authorid", "author_id", "userid", "user_id"],
        COL_CONTENT: ["content", "text", "posttext", "message"],
        COL_URL: ["url", "link", "posturl"],
        COL_POSTED: ["postedat", "posted_at", "postdate", "date", "posted"],
        COL_NARR: ["narrative", "narratives"],
        COL_MH: ["misinformation/hate", "misinformationhate", "misinformation_hate", "mh"],
        COL_ENG: ["totalengagement", "total_engagement", "engagement", "totalengagementsum"],
        COL_CLEAN: ["cleancontent", "clean_content", "clean"],
        COL_CLUSTER: ["clusterid", "cluster_id", "cluster"],
        COL_REP: ["clusterrepresentative", "cluster_representative", "representative"],
        COL_SIZE: ["clustersize", "cluster_size", "size"],
    }

    rename = {}
    for canonical, keys in want.items():
        if canonical in d.columns:
            continue
        found = None
        for k in keys:
            nk = _norm_key(k)
            if nk in colmap:
                found = colmap[nk]
                break
        if found:
            rename[found] = canonical

    if rename:
        d = d.rename(columns=rename)

    d.columns = [str(c).strip() for c in d.columns]
    return d


def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all canonical columns exist; also normalize types.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()

    for c in [
        COL_POSTID, COL_AUTHOR, COL_AUTHORID, COL_CONTENT, COL_URL, COL_POSTED,
        COL_NARR, COL_MH, COL_ENG, COL_CLEAN, COL_CLUSTER, COL_REP, COL_SIZE
    ]:
        if c not in d.columns:
            d[c] = ""

    # Normalize types
    d[COL_POSTID] = d[COL_POSTID].astype(str).str.strip()
    d[COL_CLUSTER] = d[COL_CLUSTER].astype(str).str.strip()
    d[COL_NARR] = d[COL_NARR].astype(str).str.strip()
    d[COL_MH] = d[COL_MH].astype(str).str.strip()
    d[COL_CLEAN] = d[COL_CLEAN].astype(str)
    d[COL_CONTENT] = d[COL_CONTENT].astype(str)
    d[COL_URL] = d[COL_URL].astype(str)
    d[COL_AUTHOR] = d[COL_AUTHOR].astype(str)
    d[COL_AUTHORID] = d[COL_AUTHORID].astype(str)
    d[COL_POSTED] = d[COL_POSTED].astype(str)

    # Engagement numeric
    d[COL_ENG] = pd.to_numeric(d[COL_ENG].astype(str).str.replace(",", ""), errors="coerce").fillna(0)

    # Cluster size numeric (optional)
    d[COL_SIZE] = pd.to_numeric(d[COL_SIZE].astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)

    # Keep only clustered rows (Cluster_ID present)
    d = d[d[COL_CLUSTER].astype(str).str.len() > 0].copy()

    return d


def cluster_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per cluster:
    - posts
    - unique narratives / top narratives
    - unique M/H / top M/H
    - avg + total engagement
    - representative
    """
    g = df.groupby(COL_CLUSTER, dropna=False)

    def top_k(series: pd.Series, k=3) -> str:
        s = series.replace("", pd.NA).dropna()
        if s.empty:
            return ""
        return " | ".join(s.value_counts().head(k).index.tolist())

    out = g.agg(
        Posts=(COL_POSTID, "count"),
        Unique_Narratives=(COL_NARR, lambda x: x.replace("", pd.NA).dropna().nunique()),
        Unique_MH=(COL_MH, lambda x: x.replace("", pd.NA).dropna().nunique()),
        Avg_Engagement=(COL_ENG, "mean"),
        Total_Engagement=(COL_ENG, "sum"),
        Representative=(COL_REP, lambda x: (x.replace("", pd.NA).dropna().iloc[0] if not x.replace("", pd.NA).dropna().empty else "")),
    ).reset_index()

    out = out.rename(columns={COL_CLUSTER: "Cluster_ID"})

    top_narr = g[COL_NARR].apply(lambda x: top_k(x, 3)).reset_index(name="Top_Narratives")
    top_mh = g[COL_MH].apply(lambda x: top_k(x, 3)).reset_index(name="Top_MH")

    top_narr = top_narr.rename(columns={COL_CLUSTER: "Cluster_ID"})
    top_mh = top_mh.rename(columns={COL_CLUSTER: "Cluster_ID"})

    out = out.merge(top_narr, on="Cluster_ID", how="left")
    out = out.merge(top_mh, on="Cluster_ID", how="left")

    # Numeric sorting for Cluster_ID
    def _cid_int(x):
        try:
            return int(str(x))
        except:
            return 10**9

    out["_cid"] = out["Cluster_ID"].apply(_cid_int)
    out = out.sort_values(["_cid", "Posts"], ascending=[True, False]).drop(columns=["_cid"])

    return out


# =========================
# WRITEBACK INDEX (CACHED)
# =========================
@st.cache_data(ttl=1800, show_spinner=False)
def get_postid_index(sheet_id: str) -> Tuple[dict, int, int]:
    """
    Cached PostID -> row index mapping for the target 'Facebook' sheet.
    Makes Apply much faster.
    """
    sh = open_sheet(sheet_id)
    ws = sh.worksheet(TARGET_TAB)

    header = [str(h).strip() for h in ws.row_values(1)]

    if COL_POSTID not in header:
        raise RuntimeError(f"'{TARGET_TAB}' is missing column: {COL_POSTID}")
    if COL_NARR not in header or COL_MH not in header:
        raise RuntimeError(f"'{TARGET_TAB}' must include columns: {COL_NARR} and {COL_MH}")

    postid_col = header.index(COL_POSTID) + 1
    narr_col = header.index(COL_NARR) + 1
    mh_col = header.index(COL_MH) + 1

    col_vals = ws.col_values(postid_col)  # includes header
    idx = {}
    for r, v in enumerate(col_vals[1:], start=2):
        pid = str(v).strip()
        if pid:
            idx[pid] = r

    return idx, narr_col, mh_col


def a1(row: int, col: int) -> str:
    return gspread.utils.rowcol_to_a1(row, col)


# =========================
# UI
# =========================
st.title("Facebook Cluster Explorer")

with st.sidebar:
    st.header("Data Source")
    sheet_id = st.text_input("Spreadsheet ID", value=DEFAULT_SHEET_ID)
    cluster_tab = st.text_input("Worksheet name", value=DEFAULT_CLUSTER_TAB)

    if st.button("Refresh data"):
        st.cache_data.clear()

    st.divider()
    st.header("Cluster filters")
    min_cluster_size = st.slider("Min cluster size", 1, 300, 2)

    mode = st.radio(
        "Show mixed clusters",
        options=[
            "All clusters",
            "Only clusters with >1 Narrative",
            "Only clusters with >1 Misinformation/Hate",
            "Only clusters with >1 Narrative OR >1 Misinformation/Hate",
            "Only clusters with >1 Narrative AND >1 Misinformation/Hate",
        ],
        index=0
    )

    st.divider()
    st.header("Bulk label fix")
    bulk_narr = st.selectbox("Set Narrative", options=["(no change)"] + NARRATIVE_OPTIONS, index=0)
    bulk_mh = st.selectbox("Set Misinformation/Hate", options=["(no change)"] + MH_OPTIONS, index=0)

    apply_scope = st.radio("Apply scope", ["Selected posts", "All posts in this cluster"], index=0)
    preview_only = st.checkbox("Preview only (don’t write)", value=False)


# ---- FAST load: detect header width, then load A1:<endcol> only
with st.spinner("Loading clustered sheet (fast mode)..."):
    end_col = detect_last_col_letter(sheet_id, cluster_tab)
    raw0 = load_tab_df_range(sheet_id, cluster_tab, end_col)

raw0 = normalize_headers(raw0)
df_raw = ensure_cols(raw0)

if df_raw.empty:
    st.warning(
        f"No usable clustered data found in '{cluster_tab}'.\n\n"
        f"Required at minimum: {COL_POSTID} and {COL_CLUSTER}."
    )
    with st.expander("Show detected columns"):
        if raw0 is not None and not raw0.empty:
            st.write(list(raw0.columns))
        else:
            st.write("No columns (sheet empty).")
    st.stop()

# Cluster summary table
with st.spinner("Building cluster summary..."):
    df_clusters = cluster_summary(df_raw)

# Apply filters
dfc = df_clusters.copy()
dfc = dfc[dfc["Posts"] >= min_cluster_size].copy()

if mode == "Only clusters with >1 Narrative":
    dfc = dfc[dfc["Unique_Narratives"] > 1]
elif mode == "Only clusters with >1 Misinformation/Hate":
    dfc = dfc[dfc["Unique_MH"] > 1]
elif mode == "Only clusters with >1 Narrative OR >1 Misinformation/Hate":
    dfc = dfc[(dfc["Unique_Narratives"] > 1) | (dfc["Unique_MH"] > 1)]
elif mode == "Only clusters with >1 Narrative AND >1 Misinformation/Hate":
    dfc = dfc[(dfc["Unique_Narratives"] > 1) & (dfc["Unique_MH"] > 1)]

# KPIs
k1, k2, k3, k4 = st.columns(4)
total_clusters = df_clusters.shape[0]
clusters_mixed_n = int((df_clusters["Unique_Narratives"] > 1).sum())
clusters_mixed_mh = int((df_clusters["Unique_MH"] > 1).sum())
clusters_mixed_both = int(((df_clusters["Unique_Narratives"] > 1) & (df_clusters["Unique_MH"] > 1)).sum())

k1.metric("Total clusters", f"{total_clusters:,}")
k2.metric("Clusters with >1 Narrative", f"{clusters_mixed_n:,}")
k3.metric("Clusters with >1 M/H", f"{clusters_mixed_mh:,}")
k4.metric("Clusters with >1 Narrative AND >1 M/H", f"{clusters_mixed_both:,}")

st.divider()

# Mixed clusters table
st.subheader("Mixed Clusters (quick triage)")
st.caption("Clusters where labels disagree inside the same cluster. Often over-merging or inconsistent labeling.")

table_cols = [
    "Cluster_ID", "Posts", "Unique_Narratives", "Top_Narratives",
    "Unique_MH", "Top_MH", "Avg_Engagement", "Total_Engagement", "Representative"
]
df_table = dfc[table_cols].copy()
df_table["Avg_Engagement"] = df_table["Avg_Engagement"].round(2)
df_table["Total_Engagement"] = df_table["Total_Engagement"].round(0).astype(int)

st.dataframe(df_table, use_container_width=True, height=360)

st.divider()

# Cluster drilldown picker
st.subheader("Cluster Drilldown")

def _short_rep(s: str, n=110) -> str:
    s = str(s or "").replace("\n", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s[:n] + ("…" if len(s) > n else "")

opts = dfc.head(3000).copy()
label_map = {
    r["Cluster_ID"]: f'{r["Cluster_ID"]} | Posts:{r["Posts"]} | N:{r["Unique_Narratives"]} MH:{r["Unique_MH"]} | {_short_rep(r["Representative"])}'
    for _, r in opts.iterrows()
}

picked = st.selectbox(
    "Pick a cluster",
    options=opts["Cluster_ID"].tolist(),
    format_func=lambda cid: label_map.get(cid, str(cid)),
)

cluster_df = df_raw[df_raw[COL_CLUSTER] == str(picked)].copy()

posts_count = len(cluster_df)
unique_n = cluster_df[COL_NARR].replace("", pd.NA).dropna().nunique()
unique_mh = cluster_df[COL_MH].replace("", pd.NA).dropna().nunique()
avg_eng = float(cluster_df[COL_ENG].mean()) if posts_count else 0

m1, m2, m3, m4 = st.columns([1, 1, 1, 1])
m1.metric("Posts", f"{posts_count:,}")
m2.metric("Unique Narrative", f"{unique_n:,}")
m3.metric("Unique M/H", f"{unique_mh:,}")
m4.metric("Avg engagement", f"{avg_eng:.0f}")

st.divider()

left, right = st.columns([1.2, 1])

with left:
    st.subheader("Texts appearing in this cluster (top repeated)")
    t = cluster_df[COL_CLEAN].fillna("").astype(str).str.strip()
    t = t[t != ""]
    vc = t.value_counts().head(25).reset_index()
    vc.columns = ["Clean_Content", "Count"]
    st.dataframe(vc, use_container_width=True, height=320)

with right:
    st.subheader("Narrative & Misinformation/Hate mix")

    narr_counts = cluster_df[COL_NARR].replace("", "Unknown").value_counts().reset_index()
    narr_counts.columns = ["Narrative", "Posts"]
    fig1 = px.bar(narr_counts, x="Narrative", y="Posts", title="Narrative")
    st.plotly_chart(fig1, use_container_width=True)

    mh_counts = cluster_df[COL_MH].replace("", "Unknown").value_counts().reset_index()
    mh_counts.columns = ["Misinformation/Hate", "Posts"]
    fig2 = px.bar(mh_counts, x="Misinformation/Hate", y="Posts", title="Misinformation/Hate")
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

st.subheader("Posts in this cluster")

query = st.text_input("Search inside this cluster (matches Clean_Content / Content)", value="")
view_df = cluster_df.copy()

if query.strip():
    q = query.strip().lower()
    mask = (
        view_df[COL_CLEAN].astype(str).str.lower().str.contains(q, na=False) |
        view_df[COL_CONTENT].astype(str).str.lower().str.contains(q, na=False)
    )
    view_df = view_df[mask].copy()

show_cols = [
    COL_POSTID, COL_AUTHOR, COL_AUTHORID, COL_POSTED,
    COL_NARR, COL_MH, COL_ENG, COL_URL, COL_CLEAN
]
grid = view_df[show_cols].copy()
grid.insert(0, "Select", False)

edited = st.data_editor(
    grid,
    use_container_width=True,
    height=420,
    disabled=[c for c in grid.columns if c != "Select"],
    column_config={
        "Select": st.column_config.CheckboxColumn("Select"),
        COL_ENG: st.column_config.NumberColumn("Total Engagement"),
        COL_URL: st.column_config.TextColumn("Url", width="large"),
        COL_CLEAN: st.column_config.TextColumn("Clean_Content", width="large"),
    }
)

selected = edited[edited["Select"] == True].copy()
st.write(f"Selected posts: **{len(selected):,}**")

csv_bytes = view_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download this cluster as CSV",
    data=csv_bytes,
    file_name=f"cluster_{picked}.csv",
    mime="text/csv",
)

st.divider()

st.subheader("Fix incorrect labels (write back to 'Facebook')")

new_narr = None if bulk_narr == "(no change)" else bulk_narr
new_mh = None if bulk_mh == "(no change)" else bulk_mh

apply_btn = st.button("✅ Apply changes", type="primary")

if apply_btn:
    if new_narr is None and new_mh is None:
        st.error("Pick at least one value (Narrative or Misinformation/Hate).")
        st.stop()

    if apply_scope == "Selected posts":
        if selected.empty:
            st.error("No posts selected. Tick some rows first.")
            st.stop()
        postids = selected[COL_POSTID].astype(str).str.strip().tolist()
    else:
        postids = cluster_df[COL_POSTID].astype(str).str.strip().tolist()

    # Cached index for fast apply
    try:
        postid_to_row, narr_col, mh_col = get_postid_index(sheet_id)
    except Exception as e:
        st.error(f"Failed building PostID index from '{TARGET_TAB}': {e}")
        st.stop()

    sh = open_sheet(sheet_id)
    try:
        target_ws = sh.worksheet(TARGET_TAB)
    except WorksheetNotFound:
        st.error(f"Target sheet '{TARGET_TAB}' not found.")
        st.stop()

    updates = []
    missing = []

    for pid in postids:
        r = postid_to_row.get(pid)
        if not r:
            missing.append(pid)
            continue
        if new_narr is not None:
            updates.append({"range": a1(r, narr_col), "values": [[new_narr]]})
        if new_mh is not None:
            updates.append({"range": a1(r, mh_col), "values": [[new_mh]]})

    st.write(f"Will update **{len(postids):,}** posts → **{len(updates):,}** cells.")
    if missing:
        st.warning(f"Missing PostID in '{TARGET_TAB}' for {len(missing)} posts. Those won’t be updated.")
        with st.expander("Show missing PostIDs (first 500)"):
            st.code("\n".join(missing[:500]))

    if preview_only:
        st.info("Preview-only enabled. No write performed.")
        st.stop()

    if not updates:
        st.warning("No updates to write (no PostID matched).")
        st.stop()

    CHUNK = 400
    prog = st.progress(0, text="Writing updates...")
    try:
        for i in range(0, len(updates), CHUNK):
            safe_batch_update(target_ws, updates[i:i + CHUNK])
            prog.progress(min(1.0, (i + CHUNK) / len(updates)))
        prog.progress(1.0, text="Done.")
        st.success("✅ Updated labels in 'Facebook' sheet.")

        # Keep behavior consistent: refresh caches after a write
        st.cache_data.clear()

    except Exception as e:
        st.error(f"Write failed: {e}")
