import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from io import StringIO
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Band Availability", layout="wide")

STATUS_OPTIONS = ["Available", "Maybe", "Unavailable"]
STATUS_SCORE = {"Available": 2, "Maybe": 1, "Unavailable": 0}
WEEKDAY = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

def daterange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def get_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def open_sheet():
    sheet_id = st.secrets["SHEET_ID"]
    gc = get_client()
    sh = gc.open_by_key(sheet_id)

    # Ensure worksheets exist
    try:
        ws_av = sh.worksheet("availability")
    except gspread.WorksheetNotFound:
        ws_av = sh.add_worksheet(title="availability", rows=2000, cols=10)
        ws_av.append_row(["date", "member", "status", "note", "updated_at"])

    try:
        ws_set = sh.worksheet("settings")
    except gspread.WorksheetNotFound:
        ws_set = sh.add_worksheet(title="settings", rows=100, cols=5)
        ws_set.append_row(["key", "value"])

    return sh, ws_av, ws_set

def ws_to_df(ws):
    vals = ws.get_all_values()
    if not vals:
        return pd.DataFrame()
    header = vals[0]
    rows = vals[1:]
    if not rows:
        return pd.DataFrame(columns=header)
    return pd.DataFrame(rows, columns=header)

def ensure_av_headers(ws_av):
    vals = ws_av.get_all_values()
    if not vals:
        ws_av.append_row(["date", "member", "status", "note", "updated_at"])
        return
    if vals[0] != ["date", "member", "status", "note", "updated_at"]:
        st.error("Sheet tab 'availability' has unexpected headers. Fix row 1 to: date,member,status,note,updated_at")
        st.stop()

def load_settings(ws_set):
    df = ws_to_df(ws_set)
    if df.empty or "key" not in df.columns or "value" not in df.columns:
        return {}
    return dict(zip(df["key"], df["value"]))

def upsert_availability(ws_av, df_av, d_iso, member, status, note):
    """
    Upsert row by (date, member). If exists, update status/note/updated_at.
    If not, append.
    """
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    if df_av.empty:
        ws_av.append_row([d_iso, member, status, note, now])
        return

    # Find existing row (note: df_av rows correspond to sheet rows starting at 2)
    mask = (df_av["date"] == d_iso) & (df_av["member"] == member)
    matches = df_av[mask]

    if not matches.empty:
        # first match row index in df
        df_idx = matches.index[0]
        sheet_row = int(df_idx) + 2  # +2 because header row is 1
        # columns: date(1), member(2), status(3), note(4), updated_at(5)
        ws_av.update(f"C{sheet_row}:E{sheet_row}", [[status, note, now]])
    else:
        ws_av.append_row([d_iso, member, status, note, now])

def to_ics_events(df_best: pd.DataFrame, title_prefix="Gig (Candidate)"):
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//BandAvail//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
    ]
    nowstamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    for _, row in df_best.iterrows():
        d = row["date"]
        dtstart = d.strftime("%Y%m%d")
        dtend = (d + timedelta(days=1)).strftime("%Y%m%d")
        uid = f"bandavail-{dtstart}-{abs(hash(str(row.to_dict())))%10_000_000}@local"

        summary = f"{title_prefix}: {d.isoformat()} (Score {row['score']})"
        desc = (
            f"Total score: {row['score']}\n"
            f"Available: {row['available_count']} | Maybe: {row['maybe_count']} | Unavailable: {row['unavailable_count']}\n"
            f"Notes: {row.get('notes','') or ''}"
        )

        lines += [
            "BEGIN:VEVENT",
            f"UID:{uid}",
            f"DTSTAMP:{nowstamp}",
            f"DTSTART;VALUE=DATE:{dtstart}",
            f"DTEND;VALUE=DATE:{dtend}",
            f"SUMMARY:{summary}",
            "DESCRIPTION:" + desc.replace("\n", "\\n"),
            "END:VEVENT",
        ]

    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"

# ---------------- UI ----------------

st.title("🎸 Band Availability (Google Sheets)")
st.caption("Everyone updates availability from their phone. Best gig dates update automatically.")

with st.spinner("Connecting to Google Sheets..."):
    sh, ws_av, ws_set = open_sheet()
    ensure_av_headers(ws_av)
    settings = load_settings(ws_set)

df_av = ws_to_df(ws_av)

# Sidebar controls
st.sidebar.header("Date range")
start = st.sidebar.date_input("Start date", value=date.today())
end = st.sidebar.date_input("End date", value=date.today() + timedelta(days=30))
if end < start:
    st.sidebar.error("End date must be on/after start date.")
    st.stop()

exclude_days = st.sidebar.multiselect(
    "Exclude weekdays (optional)",
    options=WEEKDAY,
    default=[]
)

gig_title = settings.get("gig_title", "Gig (Candidate)")

dates = [d for d in daterange(start, end)]
if exclude_days:
    dates = [d for d in dates if WEEKDAY[d.weekday()] not in set(exclude_days)]
if not dates:
    st.warning("No dates left after filtering.")
    st.stop()

# Member selection (bandmates do this on mobile)
st.subheader("1) Pick your name and set availability")
member = st.text_input("Your name", placeholder="e.g., Aoife / Dave / The Drummer")

if not member.strip():
    st.info("Enter your name to start.")
    st.stop()

member = member.strip()

# Build quick lookup for this member
# df_av columns are strings (since sheet returns strings)
def get_status_for(d_iso):
    if df_av.empty:
        return "Maybe"
    m = (df_av["date"] == d_iso) & (df_av["member"] == member)
    hit = df_av[m]
    if hit.empty:
        return "Maybe"
    s = hit.iloc[-1]["status"] or "Maybe"
    return s if s in STATUS_OPTIONS else "Maybe"

def get_note_for(d_iso):
    if df_av.empty:
        return ""
    m = (df_av["date"] == d_iso) & (df_av["member"] == member)
    hit = df_av[m]
    if hit.empty:
        return ""
    return hit.iloc[-1].get("note", "") or ""

# Edit availability in chunks (mobile-friendly-ish)
chunk = st.radio("Show dates in:", ["7-day chunks", "14-day chunks", "All"], horizontal=True)
chunk_size = 7 if chunk == "7-day chunks" else 14 if chunk == "14-day chunks" else len(dates)

saved_any = False
for i in range(0, len(dates), chunk_size):
    block = dates[i:i + chunk_size]
    cols = st.columns(len(block))
    for j, d in enumerate(block):
        d_iso = d.isoformat()
        with cols[j]:
            st.caption(f"{d.strftime('%d %b')} ({WEEKDAY[d.weekday()]})")
            default_status = get_status_for(d_iso)
            status = st.selectbox(
                "Status",
                STATUS_OPTIONS,
                index=STATUS_OPTIONS.index(default_status),
                key=f"status_{member}_{d_iso}",
                label_visibility="collapsed",
            )
            note = st.text_input(
                "Note",
                value=get_note_for(d_iso),
                key=f"note_{member}_{d_iso}",
                placeholder="(optional)",
                label_visibility="collapsed",
            )
            if st.button("Save", key=f"save_{member}_{d_iso}"):
                upsert_availability(ws_av, df_av, d_iso, member, status, note)
                saved_any = True

if saved_any:
    st.success("Saved to Google Sheets ✅ Refreshing scores...")
    df_av = ws_to_df(ws_av)

st.divider()
st.subheader("2) Best dates (ranked)")

# Compute best dates from sheet data
# Normalize types
if not df_av.empty:
    # Keep last update per (date, member)
    df_av2 = df_av.copy()
    df_av2["updated_at"] = df_av2["updated_at"].fillna("")
    df_av2 = df_av2.sort_values(by=["date", "member", "updated_at"])
    df_av2 = df_av2.drop_duplicates(subset=["date", "member"], keep="last")
else:
    df_av2 = pd.DataFrame(columns=["date", "member", "status", "note", "updated_at"])

members = sorted(df_av2["member"].unique().tolist()) if not df_av2.empty else [member]

rows = []
for d in dates:
    d_iso = d.isoformat()
    day = WEEKDAY[d.weekday()]

    statuses = []
    notes = []
    for m in members:
        hit = df_av2[(df_av2["date"] == d_iso) & (df_av2["member"] == m)]
        if hit.empty:
            s = "Maybe"
            n = ""
        else:
            s = hit.iloc[0]["status"] if hit.iloc[0]["status"] in STATUS_OPTIONS else "Maybe"
            n = hit.iloc[0].get("note", "") or ""
        statuses.append(s)
        if n:
            notes.append(f"{m}: {n}")

    score = sum(STATUS_SCORE[s] for s in statuses)
    rows.append({
        "date": d,
        "weekday": day,
        "score": score,
        "available_count": sum(1 for s in statuses if s == "Available"),
        "maybe_count": sum(1 for s in statuses if s == "Maybe"),
        "unavailable_count": sum(1 for s in statuses if s == "Unavailable"),
        "notes": " | ".join(notes),
    })

df_best = pd.DataFrame(rows).sort_values(
    by=["score", "available_count", "maybe_count"],
    ascending=[False, False, False]
).reset_index(drop=True)

top_n = st.slider("Show top N dates", 1, min(30, len(df_best)), min(10, len(df_best)))
df_best_n = df_best.head(top_n)

st.dataframe(df_best_n, use_container_width=True, hide_index=True)

st.caption("Scoring: Available=2, Maybe=1, Unavailable=0. Ties break by more Availables, then Maybes.")

st.divider()
st.subheader("3) Export")
csv_buf = StringIO()
df_best_n.assign(date=df_best_n["date"].astype(str)).to_csv(csv_buf, index=False)
st.download_button(
    "⬇️ Download CSV (best dates)",
    data=csv_buf.getvalue().encode("utf-8"),
    file_name="best_gig_dates.csv",
    mime="text/csv",
)

ics_text = to_ics_events(df_best_n, title_prefix=gig_title)
st.download_button(
    "⬇️ Download iCal (.ics) (best dates)",
    data=ics_text.encode("utf-8"),
    file_name="best_gig_dates.ics",
    mime="text/calendar",
)

st.divider()
with st.expander("Admin / debug"):
    st.write("Sheet:", sh.title)
    st.write("Members detected from sheet:", ", ".join(members))
    st.write("Rows in availability:", 0 if df_av2.empty else len(df_av2))