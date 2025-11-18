# Distributed Job Scheduler - MVP

Run locally:
1. python3 -m venv venv
2. source venv/bin/activate    # Windows: venv\Scripts\activate
3. pip install -r requirements.txt
4. python app.py
5. Visit http://127.0.0.1:5000/health

Create a job (example):
curl -X POST http://127.0.0.1:5000/jobs -H "Content-Type: application/json" -d '{"delay":5,"payload":"hello"}'

List jobs:
curl http://127.0.0.1:5000/jobs
