from celery.utils.log import get_task_logger
from kombu import Queue, Exchange
from dotenv import load_dotenv
from celery import Celery
from pathlib import Path
import os

logger = get_task_logger(__name__)

# LOAD ENVIRONMENT VARIABLES
ENV_FILE = Path(".env")

load_dotenv(ENV_FILE)

# CONFIGURATION
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
DEFAULT_RETRY_DELAY = int(os.getenv("DEFAULT_RETRY_DELAY", 5))
RETRY_BACKOFF_BASE = int(os.getenv("RETRY_BACKOFF_BASE", 2))
MAX_RETRY_DELAY = int(os.getenv("MAX_RETRY_DELAY", 3600))

# CELERY SETUP
dlq_exchange = Exchange("dlq", type="direct")

celery_app = Celery(
    "app",
    broker=os.getenv("BROKER_URL", "sqla+sqlite:///db.sqlite"),
    backend=os.getenv("BACKEND_URL", "db+sqlite:///db.sqlite"),
)

celery_app.conf.task_queues = (
    Queue("default", exchange="default", routing_key="default", durable=True),
    Queue("dlq", exchange=dlq_exchange, routing_key="dlq", durable=True),
)
celery_app.conf.task_default_queue = "default"
celery_app.conf.task_default_exchange = "default"
celery_app.conf.task_default_routing_key = "default"
celery_app.conf.task_default_delivery_mode = 2

celery_app.autodiscover_tasks(['apps.jobs'])

def set_config(key: str, value: int):
    global MAX_RETRIES, BACKOFF_BASE

    if key not in ["max_retries", "backoff_base"]:
        print(f"Invalid config key '{key}'. Valid keys: max_retries, backoff_base")
        return

    # Update environment variable and rewrite .env file
    os.environ[key.upper()] = str(value)

    env_lines = []
    with ENV_FILE.open("r") as f:
        for line in f:
            if line.startswith(f"{key.upper()}="):
                env_lines.append(f"{key.upper()}={value}\n")
            else:
                env_lines.append(line)
    with ENV_FILE.open("w") as f:
        f.writelines(env_lines)

    if key == "max_retries":
        MAX_RETRIES = int(value)
    else:
        BACKOFF_BASE = int(value)

    print(f"Config updated: {key} = {value}")
    return {"status": "ok", "message": f"Config updated: {key} = {value}"}