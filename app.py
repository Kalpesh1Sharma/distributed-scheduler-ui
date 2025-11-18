# app.py
import os
import time
import uuid
import heapq
import threading
import sqlite3
import logging
from flask import Flask, request, jsonify

# --- Config & logging ---
DB_FILE = os.environ.get('DB_FILE', 'jobs.db')
PORT = int(os.environ.get('PORT', 5000))
LOG = logging.getLogger("scheduler")
LOG.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
LOG.addHandler(handler)

# --- Flask app ---
app = Flask(__name__)

# In-memory structures
heap = []                 # min-heap of (run_at, job_id)
heap_lock = threading.Lock()
jobs = {}                 # job_id -> job dict
jobs_lock = threading.Lock()

# --- SQLite persistence helpers ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        run_at REAL,
        payload TEXT,
        status TEXT,
        retries INTEGER,
        recurring INTEGER,
        interval REAL
      )
    ''')
    conn.commit()
    conn.close()
    LOG.info("DB initialized (%s)", DB_FILE)

def persist_job(job):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('REPLACE INTO jobs (id, run_at, payload, status, retries, recurring, interval) VALUES (?,?,?,?,?,?,?)',
              (job['id'], job['run_at'], job.get('payload',''), job['status'], job.get('retries',0),
               int(job.get('recurring',0)), job.get('interval', 0.0)))
    conn.commit()
    conn.close()

def load_jobs_at_start():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT id, run_at, payload, status, retries, recurring, interval FROM jobs WHERE status != "done"')
    rows = c.fetchall()
    for row in rows:
        job = {
            'id': row[0],
            'run_at': row[1],
            'payload': row[2],
            'status': row[3],
            'retries': row[4],
            'recurring': bool(row[5]),
            'interval': row[6]
        }
        jobs[job['id']] = job
        with heap_lock:
            heapq.heappush(heap, (job['run_at'], job['id']))
    conn.close()
    LOG.info("Loaded %d jobs from DB", len(rows))

# --- Scheduler loop ---
def scheduler_loop():
    LOG.info("Scheduler thread started")
    while True:
        now = time.time()
        next_job = None
        with heap_lock:
            if heap and heap[0][0] <= now:
                _, job_id = heapq.heappop(heap)
                next_job = jobs.get(job_id)
        if next_job:
            threading.Thread(target=execute_job, args=(next_job,), daemon=True).start()
        else:
            time.sleep(0.2)

def execute_job(job):
    job_id = job['id']
    try:
        LOG.info("[EXEC] Running job %s payload=%s", job_id, job.get('payload'))
        # --- Simulate actual work here. Replace with real logic if needed.
        time.sleep(0.5)
        # mark done
        job['status'] = 'done'
        persist_job(job)
        LOG.info("[DONE] Job %s completed", job_id)

        # schedule recurring if needed
        if job.get('recurring'):
            job['run_at'] = time.time() + float(job.get('interval', 0.0))
            job['status'] = 'scheduled'
            with heap_lock:
                heapq.heappush(heap, (job['run_at'], job_id))
            persist_job(job)
            LOG.info("[RECUR] Rescheduled recurring job %s next=%s", job_id, job['run_at'])

    except Exception as e:
        LOG.exception("Job failed: %s", e)
        job['retries'] = job.get('retries', 0) + 1
        if job['retries'] <= 3:
            job['status'] = 'scheduled'
            job['run_at'] = time.time() + 2  # retry after 2s
            with heap_lock:
                heapq.heappush(heap, (job['run_at'], job_id))
            persist_job(job)
            LOG.info("[RETRY] Job %s scheduled to retry (attempt %d)", job_id, job['retries'])
        else:
            job['status'] = 'dead'
            persist_job(job)
            LOG.info("[DLQ] Job %s moved to dead-letter after retries", job_id)

# --- HTTP API endpoints ---
@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status':'ok'}), 200

@app.route('/jobs', methods=['POST'])
def create_job():
    data = request.json or {}
    delay = float(data.get('delay', 0))          # seconds from now
    run_at = time.time() + delay
    job_id = str(uuid.uuid4())
    job = {
        'id': job_id,
        'run_at': run_at,
        'payload': data.get('payload', ''),
        'status': 'scheduled',
        'retries': 0,
        'recurring': bool(data.get('recurring', False)),
        'interval': float(data.get('interval', 0))
    }
    with jobs_lock:
        jobs[job_id] = job
    with heap_lock:
        heapq.heappush(heap, (run_at, job_id))
    persist_job(job)
    LOG.info("Created job %s run_at=%s payload=%s", job_id, run_at, job['payload'])
    return jsonify({'id': job_id, 'run_at': run_at}), 201

@app.route('/jobs', methods=['GET'])
def list_jobs():
    with jobs_lock:
        return jsonify(list(jobs.values()))

@app.route('/jobs/<job_id>', methods=['GET'])
def get_job(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({'error':'not found'}), 404
    return jsonify(job)

@app.route('/jobs/<job_id>', methods=['DELETE'])
def delete_job(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({'error':'not found'}), 404
    job['status'] = 'cancelled'
    persist_job(job)
    LOG.info("Cancelled job %s", job_id)
    return jsonify({'ok': True})

# --- Boot sequence ---
if __name__ == '__main__':
    init_db()
    load_jobs_at_start()
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
    LOG.info("Starting Flask on 0.0.0.0:%d", PORT)
    # bind to 0.0.0.0 and use PORT env var
    app.run(host='0.0.0.0', port=PORT)
