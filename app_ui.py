# streamlit_app.py
import streamlit as st
import requests
import time
from datetime import datetime

st.set_page_config(page_title="Job Scheduler UI", layout="wide")
st.title("Distributed Job Scheduler — Demo UI")

# --- Config ---
API_BASE ="https://distributed-scheduler.onrender.com"
# optional: allow overriding with text input (useful for local testing)
if "override_api" not in st.session_state:
    st.session_state.override_api = ""
api_base = st.session_state.override_api or API_BASE

st.markdown(
    "This UI calls the scheduler backend. Make sure the backend URL is set in Streamlit secrets as API_BASE."
)

# helpers
def pretty_time(ts):
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)

def api_get(path, timeout=10):
    return requests.get(f"{api_base}{path}", timeout=timeout)

def api_post(path, json, timeout=10):
    return requests.post(f"{api_base}{path}", json=json, timeout=timeout)

def api_delete(path, timeout=10):
    return requests.delete(f"{api_base}{path}", timeout=timeout)

# Sidebar: create job
with st.sidebar:
    st.header("Create job")
    st.text_input("Override backend URL (optional)", key="override_api")
    delay = st.number_input("Delay (seconds)", min_value=0, value=5, step=1)
    payload = st.text_input("Payload", value="hello-from-streamlit")
    recurring = st.checkbox("Recurring", value=False)
    interval = st.number_input("Recurring interval (seconds)", min_value=0, value=60, step=1)
    if st.button("Create Job"):
        body = {
            "delay": int(delay),
            "payload": payload,
            "recurring": bool(recurring),
            "interval": float(interval or 0)
        }
        try:
            r = api_post("/jobs", body)
            r.raise_for_status()
            data = r.json()
            st.success(f"Created job {data.get('id')}")
            st.session_state.last_job_id = data.get("id")
        except Exception as e:
            st.error(f"Create job failed: {e}")

st.write("---")

# top row: health + quick actions
col1, col2, col3 = st.columns([1, 2, 2])

with col1:
    st.subheader("Backend Health")
    try:
        r = api_get("/health", timeout=5)
        if r.status_code == 200:
            st.success("OK")
        else:
            st.error(f"Status {r.status_code}")
    except Exception as e:
        st.error(f"Error: {e}")

with col2:
    st.subheader("Quick actions")
    if st.button("Refresh jobs"):
        st.experimental_rerun()
    if st.button("Create demo jobs (5)"):
        # create 5 demo jobs quickly
        for i in range(1, 6):
            try:
                api_post("/jobs", {"delay": i * 2, "payload": f"demo-{i}"})
            except Exception:
                pass
        st.success("Posted 5 demo jobs (watch logs).")

with col3:
    st.subheader("Last created")
    last = st.session_state.get("last_job_id", None)
    if last:
        st.write(f"Job id: {last}")
        if st.button("Open last job (GET)"):
            try:
                r = api_get(f"/jobs/{last}")
                r.raise_for_status()
                st.json(r.json())
            except Exception as e:
                st.error(f"Fetch failed: {e}")
    else:
        st.write("No job created yet in this session.")

st.write("---")

# Main: jobs table / details
st.subheader("Jobs")
jobs = []
try:
    r = api_get("/jobs", timeout=10)
    r.raise_for_status()
    jobs = r.json()
except Exception as e:
    st.error(f"Could not fetch jobs: {e}")

if jobs:
    # Render as table-like UI with actions
    for j in jobs:
        j_id = j.get("id")
        status = j.get("status")
        run_at = pretty_time(j.get("run_at"))
        payload = j.get("payload", "")
        with st.expander(f"ID: {j_id} — status: {status} — run_at: {run_at}", expanded=False):
            st.write("Payload:", payload)
            cols = st.columns([1, 1, 1, 2])
            if cols[0].button("Show", key=f"show_{j_id}"):
                st.json(j)
            if cols[1].button("Run now", key=f"run_{j_id}"):
                # create immediate job with same payload
                try:
                    resp = api_post("/jobs", {"delay": 0, "payload": payload})
                    resp.raise_for_status()
                    st.success("Triggered immediate run")
                except Exception as e:
                    st.error(f"Failed to trigger: {e}")
            if cols[2].button("Cancel", key=f"cancel_{j_id}"):
                try:
                    resp = api_delete(f"/jobs/{j_id}")
                    resp.raise_for_status()
                    st.success("Cancelled")
                except Exception as e:
                    st.error(f"Cancel failed: {e}")
            # show metadata
            st.markdown(f"- *Status:* {status}")
            st.markdown(f"- *Run at:* {run_at}")
            st.markdown(f"- *Retries:* {j.get('retries', 0)}")
else:
    st.info("No jobs found. Use the sidebar to create a job.")