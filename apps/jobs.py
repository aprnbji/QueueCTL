import time, json, sqlite3, uuid
from datetime import datetime
from kombu import Queue, Exchange, Connection, Producer
from pydantic import BaseModel
from pathlib import Path

from apps.config import (
    celery_app, dlq_exchange, logger,
    MAX_RETRIES, DEFAULT_RETRY_DELAY,
    RETRY_BACKOFF_BASE, MAX_RETRY_DELAY,
)

# ---------------------------------------------------------------------------
# DATABASE / JOB STORE
# ---------------------------------------------------------------------------

DB_FILE = Path("jobs.sqlite")

class JobStore:
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    task_id TEXT PRIMARY KEY,
                    job_name TEXT,
                    description TEXT,
                    state TEXT,
                    result TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dlq (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT,
                    job_name TEXT,
                    description TEXT,
                    failed_at TEXT,
                    attempts INTEGER,
                    last_error TEXT
                )
            """)

    def add_job(self, task_id, job_name, description):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO jobs (task_id, job_name, description, state, created_at, updated_at)
                VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
            """, (task_id, job_name, description, "QUEUED"))

    def update_job(self, task_id, state, result=None):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE jobs
                SET state=?, result=?, updated_at=datetime('now')
                WHERE task_id=?
            """, (state, json.dumps(result) if result else None, task_id))

    def add_dlq_entry(self, job):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO dlq (task_id, job_name, description, failed_at, attempts, last_error)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                job["task_id"],
                job["job_name"],
                job["description"],
                job["failed_at"],
                job["attempts"],
                job["last_error"],
            ))

    def list_jobs(self):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC")
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_dlq(self):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT * FROM dlq ORDER BY failed_at DESC")
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


job_store = JobStore()

# ---------------------------------------------------------------------------
# TASKS
# ---------------------------------------------------------------------------

@celery_app.task(bind=True, max_retries=MAX_RETRIES, default_retry_delay=DEFAULT_RETRY_DELAY)
def process_job_task(self, job_name: str, description: str):
    task_id = getattr(self.request, "id", None)
    logger.info(f"Processing job '{job_name}' - description: {description} (task_id={task_id})")

    job_store.update_job(task_id, "STARTED")

    try:
        for i in range(5):
            time.sleep(1)
            self.update_state(state="PROGRESS", meta={"progress": (i + 1) * 20})
            job_store.update_job(task_id, "PROGRESS", {"progress": (i + 1) * 20})

        if job_name == "fail":
            raise RuntimeError("Simulated failure")

        result = f"Job '{job_name}' completed successfully."
        logger.info(result)
        job_store.update_job(task_id, "SUCCESS", {"message": result, "description": description})
        return {"message": result, "description": description}

    except Exception as exc:
        attempts = getattr(self.request, "retries", 0) + 1
        delay = int(min(RETRY_BACKOFF_BASE ** attempts, MAX_RETRY_DELAY))

        if attempts >= MAX_RETRIES:
            logger.error(f"Job '{job_name}' hit max retries ({attempts}). Sending to DLQ.")
            with Connection(celery_app.connection().as_uri()) as conn:
                producer = Producer(conn)
                dlq_payload = {
                    "job_name": job_name,
                    "description": description,
                    "failed_at": datetime.utcnow().isoformat(),
                    "attempts": attempts,
                    "last_error": str(exc),
                    "task_id": task_id,
                }
                producer.publish(
                    dlq_payload,
                    exchange=dlq_exchange,
                    routing_key="dlq",
                    declare=[Queue("dlq", exchange=dlq_exchange, routing_key="dlq", durable=True)],
                    serializer="json",
                    delivery_mode=2,
                )
                job_store.add_dlq_entry(dlq_payload)

            job_store.update_job(task_id, "FAILED", {"error": str(exc)})
            raise
        else:
            logger.warning(f"Job '{job_name}' failed (attempt {attempts}/{MAX_RETRIES}): {exc}. Retrying in {delay}s.")
            job_store.update_job(task_id, "RETRY", {"error": str(exc), "retry_in": delay})
            raise self.retry(exc=exc, countdown=delay)

# ---------------------------------------------------------------------------
# JOB MANAGEMENT API
# ---------------------------------------------------------------------------

class JobRequest(BaseModel):
    job_name: str
    description: str


def submit_job(request: JobRequest):
    task_id = str(uuid.uuid4())
    job_store.add_job(task_id, request.job_name, request.description)
    process_job_task.apply_async(args=[request.job_name, request.description], task_id=task_id)
    return {"task_id": task_id, "status": "queued"}


def job_status(task_id: str):
    task = celery_app.AsyncResult(task_id)
    result = getattr(task, "result", None)
    if isinstance(result, BaseException):
        result = str(result)
    meta = getattr(task, "info", None)
    try:
        json.dumps(meta)
    except TypeError:
        meta = str(meta)
    return {"task_id": task_id, "state": task.state, "result": result, "meta": meta}


def retry_dlq(task_id: str):
    failed_jobs = job_store.list_dlq()
    job = None

    for j in failed_jobs:
        if j["task_id"] == task_id:
            job = j
            break

    if not job:
        logger.warning(f"No DLQ entry found for task_id={task_id}")
        return {"status": "error", "message": f"No DLQ entry found for task_id {task_id}"}

    logger.info(f"Re-enqueuing job '{job['job_name']}' from DLQ table (task_id={task_id})")
    process_job_task.apply_async(args=[job["job_name"], job["description"]], task_id=task_id)
    return {"retried": task_id}



def list_jobs():
    return job_store.list_jobs()

def list_dlq():
    return job_store.list_dlq()