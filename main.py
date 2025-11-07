from fastapi import FastAPI
from pydantic import BaseModel, Field
from celery import Celery
from celery.result import AsyncResult
import time
import uuid
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="QueueCTL")

celery_app = Celery(
    "tasks",
    broker="sqla+sqlite:///db.sqlite",
    backend="db+sqlite:///db.sqlite"
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,   
    task_acks_late=True,       
    result_expires=3600,       
)

@celery_app.task(bind=True, name="process_job")
def process_job_task(self, job_name: str, description: str):
    logger.info(f"Processing job '{job_name}'")
    for i in range(5):
        time.sleep(1)

        self.update_state(state="PROGRESS", meta={"progress": (i + 1) * 20})
    result = f"Job '{job_name}' completed successfully. Description: {description}"
    logger.info(f"Completed job '{job_name}'")
    return {"message": result}


class JobRequest(BaseModel):
    job_name: str = Field(..., description="Name of the job")
    description: str = Field(..., description="Job description")

# enqueue a new background job
@app.post("/enqueue/")
async def submit_job(request: JobRequest):

    task_id = str(uuid.uuid4())
    process_job_task.apply_async(args=[request.job_name, request.description], task_id=task_id)
    return {"task_id": task_id, "status": "queued"}


@app.get("/status/{task_id}")
async def job_status(task_id: str):
    """
    Check job status by task ID.
    """
    task = AsyncResult(task_id, app=celery_app)

    if task.state == "PENDING":
        return {"task_id": task_id, "state": "pending"}
    elif task.state in ["STARTED", "PROGRESS"]:
        return {"task_id": task_id, "state": "processing", "meta": task.info}
    elif task.state == "SUCCESS":
        return {"task_id": task_id, "state": "completed", "result": task.result}
    elif task.state == "FAILURE":
        return {"task_id": task_id, "state": "failed", "error": str(task.result)}
    elif task.state == "REVOKED":
        return {"task_id": task_id, "state": "dead"}
    else:
        return {"task_id": task_id, "state": task.state.lower()}


@app.get("/health")
async def health():
    """
    Health check endpoint.
    """
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
