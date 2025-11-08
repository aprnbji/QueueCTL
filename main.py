from dependencies import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TASK_REGISTRY = {}
DEAD_LETTER_QUEUE = {}

PROCESS_LOCKS = set()
LOCK = threading.Lock()

CONFIG_FILE = Path("config.json")
WORKERS_FILE = Path("workers.json")

def load_config():
    if not CONFIG_FILE.exists():
        raise RuntimeError(
            "Configuration missing. Create config.json with "
            "'max_retries' and 'backoff_base' values."
        )

    with CONFIG_FILE.open("r") as f:
        cfg = json.load(f)

    if "max_retries" not in cfg or "backoff_base" not in cfg:
        raise RuntimeError("config.json missing required keys: max_retries, backoff_base")

    return cfg


CONFIG = load_config()
MAX_RETRIES = int(CONFIG["max_retries"])
BACKOFF_BASE = int(CONFIG["backoff_base"])

def set_config(key: str, value: int):
    global MAX_RETRIES, BACKOFF_BASE
    if key not in ["max_retries", "backoff_base"]:
        print(f"Invalid config key '{key}'. Valid keys: max_retries, backoff_base")
        return
    CONFIG[key] = value
    if key == "max_retries":
        MAX_RETRIES = int(value)
    else:
        BACKOFF_BASE = int(value)
    print(f"Config updated: {key} = {value}")



def handle_shutdown(signum, frame):
    logger.info("Worker received shutdown signal. Will finish current task before exit.")

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

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


@celery_app.task(bind=True, name="process_job", max_retries=MAX_RETRIES)
def process_job_task(self, job_name: str, description: str):
    job_key = f"{job_name}:{description}"

    with LOCK:
        if job_key in PROCESS_LOCKS:
            logger.warning(f"Duplicate detected — skipping job: {job_key}")
            return {"message": "duplicate skipped"}
        PROCESS_LOCKS.add(job_key)

    logger.info(f"Processing job '{job_name}'")

    try:
        for i in range(5):
            time.sleep(1)
            self.update_state(state="PROGRESS", meta={"progress": (i + 1) * 20})

        if job_name == "fail":
            raise RuntimeError("Simulated failure")

        result = f"Job '{job_name}' completed successfully. Description: {description}"
        logger.info(result)
        return {"message": result}

    except Exception as exc:
        attempts = self.request.retries + 1
        delay = BACKOFF_BASE ** attempts
        logger.error(f"Error: {exc}. Retrying in {delay}s")
        raise self.retry(exc=exc, countdown=delay)

    finally:
        with LOCK:
            PROCESS_LOCKS.discard(job_key)

# job management
class JobRequest(BaseModel):
    job_name: str = Field(..., description="Name of the job")
    description: str = Field(..., description="Job description")


def submit_job(request: JobRequest):
    task_id = str(uuid.uuid4())
    process_job_task.apply_async(args=[request.job_name, request.description], task_id=task_id)

    TASK_REGISTRY[task_id] = {
        "job_name": request.job_name,
        "description": request.description,
        "state": "queued",
        "created_at": datetime.utcnow().isoformat(),
    }

    return {"task_id": task_id, "status": "queued"}


def list_jobs_by_state(state: str):
    if state is None:
        return {"count": len(TASK_REGISTRY), "jobs": TASK_REGISTRY}
    
    state = state.lower()
    filtered = [
        {"task_id": task_id, **meta}
        for task_id, meta in TASK_REGISTRY.items()
        if meta["state"].lower() == state
    ]
    return {"count": len(filtered), "jobs": filtered}


def job_status(task_id: str):
    task = AsyncResult(task_id, app=celery_app)
    state = task.state

    if task_id in TASK_REGISTRY:
        TASK_REGISTRY[task_id]["state"] = state

    if state == "FAILURE":
        if task_id in TASK_REGISTRY:
            DEAD_LETTER_QUEUE[task_id] = TASK_REGISTRY[task_id]
            DEAD_LETTER_QUEUE[task_id]["failed_at"] = datetime.utcnow().isoformat()

    if state == "PENDING":
        return {"task_id": task_id, "state": "pending"}
    elif state in ["STARTED", "PROGRESS"]:
        return {"task_id": task_id, "state": "processing", "meta": task.info}
    elif state == "SUCCESS":
        return {"task_id": task_id, "state": "completed", "result": task.result}
    elif state == "FAILURE":
        return {"task_id": task_id, "state": "failed", "error": str(task.result)}
    elif state == "REVOKED":
        return {"task_id": task_id, "state": "dead"}

    return {"task_id": task_id, "state": state}

def list_dlq():
    return {"count": len(DEAD_LETTER_QUEUE), "jobs": DEAD_LETTER_QUEUE}

def retry_dlq(task_id: str):
    if task_id not in DEAD_LETTER_QUEUE:
        return {"status": "error", "message": f"No job with ID '{task_id}' found in DLQ."}

    job = DEAD_LETTER_QUEUE.pop(task_id)
    process_job_task.apply_async(args=[job["job_name"], job["description"]], task_id=task_id)
    TASK_REGISTRY[task_id] = {
        "job_name": job["job_name"],
        "description": job["description"],
        "state": "queued",
        "created_at": datetime.utcnow().isoformat(),
    }
    return {"status": "success", "message": f"Job '{task_id}' re-enqueued successfully."}


def health():
    return {"status": "ok"}

#worker management
def load_workers():
    if WORKERS_FILE.exists():
        return json.loads(WORKERS_FILE.read_text())
    return {}


def save_workers(data):
    WORKERS_FILE.write_text(json.dumps(data, indent=4))


def start_workers(count):
    workers = load_workers()
    for i in range(1, count + 1):
        name = f"worker_{i}"
        if name in workers:
            print(f"{name} already running (PID {workers[name]['pid']})")
            continue

        print(f"Starting {name}...")

        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "celery",
                "-A",
                Path(__file__).stem,
                "worker",
                "--loglevel=info",
                "-n",
                name,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        workers[name] = {"pid": process.pid}
        print(f"Started {name} with PID {process.pid}")

    save_workers(workers)


def stop_worker(name):
    workers = load_workers()
    if name not in workers:
        print(f"No worker named {name}")
        return

    pid = workers[name]["pid"]
    print(f"Stopping {name} (PID {pid})...")

    if psutil.pid_exists(pid):
        os.kill(pid, signal.SIGTERM)
        print(f"Stopped.")
    else:
        print("Worker not running.")

    del workers[name]
    save_workers(workers)


def list_workers():
    workers = load_workers()
    if not workers:
        print("No workers found.")
        return

    print("\nWorkers:")
    for name, info in workers.items():
        pid = info["pid"]
        status = "running" if psutil.pid_exists(pid) else "not running"
        print(f" - {name}: PID {pid} ({status})")

# cli

def main():
    parser = argparse.ArgumentParser(description="QueueCTL")
    sub = parser.add_subparsers(dest="cmd")

    # job management
    j = sub.add_parser("job")
    jsub = j.add_subparsers(dest="jcmd")

    # enqueue
    jenq = jsub.add_parser("enqueue")
    jenq.add_argument("--job", required=True, help="job name")
    jenq.add_argument("--desc", default="", help="description")

    # list
    jlist = jsub.add_parser("list")
    jlist.add_argument("--state", default=None)

    # status
    jstat = jsub.add_parser("status")
    jstat.add_argument("id")

    # dlq
    jdlq = jsub.add_parser("dlq")
    jdlq_sub = jdlq.add_subparsers(dest="dlq_cmd")  
    jdlq_sub.add_parser("list")
    retry_parser = jdlq_sub.add_parser("retry")
    retry_parser.add_argument("id", help="Task ID to retry from DLQ") 

    # worker management
    w = sub.add_parser("worker")
    wsub = w.add_subparsers(dest="wcmd")

    start = wsub.add_parser("start")
    start.add_argument("--count", type=int, default=1)

    stop = wsub.add_parser("stop")
    stop.add_argument("name")

    wsub.add_parser("list")

    # config management
    c = sub.add_parser("config")  
    csub = c.add_subparsers(dest="ccmd")  
    set_cmd = csub.add_parser("set")  
    set_cmd.add_argument("key", help="Config key: max_retries or backoff_base") 
    set_cmd.add_argument("value", type=int, help="New value for the config key") 

    args = parser.parse_args()

    if args.cmd == "worker":
        if args.wcmd == "start":
            start_workers(args.count)
        elif args.wcmd == "stop":
            stop_worker(args.name)
        elif args.wcmd == "list":
            list_workers()
        else:
            parser.print_help()
    
    elif args.cmd == "job":
        if args.jcmd == "enqueue":
            req = JobRequest(job_name=args.job, description=args.desc)
            print(json.dumps(submit_job(req), indent=2))

        elif args.jcmd == "list":
            print(json.dumps(list_jobs_by_state(args.state), indent=2))

        elif args.jcmd == "status":
            print(json.dumps(job_status(args.id), indent=2))

        elif args.jcmd == "dlq":
            if args.dlq_cmd == "list" or args.dlq_cmd is None:
                print(json.dumps(list_dlq(), indent=2))
            elif args.dlq_cmd == "retry":
                print(json.dumps(retry_dlq(args.id), indent=2))

            else:
                parser.print_help()

        else:
            parser.print_help()
    
    elif args.cmd == "config":
        if args.ccmd == "set":
            print(json.dumps(set_config(args.key, args.value), indent=2))
        else:
            parser.print_help()

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
