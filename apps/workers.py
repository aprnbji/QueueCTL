from pathlib import Path
import subprocess
import psutil
import signal
import json
import sys
import os

WORKERS_FILE = Path("workers.json")


def load_workers():
    if WORKERS_FILE.exists():
        with open(WORKERS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_workers(data):
    with open(WORKERS_FILE, "w") as f:
        json.dump(data, f, indent=4)


def terminate_worker(name, pid):
    if not psutil.pid_exists(pid):
        print(f"Worker {name} (PID {pid}) not running.")
        return

    print(f"Stopping {name} (PID {pid})...")
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Stopped {name} successfully.")
    except Exception as e:
        print(f"Error: stopping {name}: {e}")


def start_workers(count):
    workers = load_workers()
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    for i in range(1, count + 1):
        worker_name = f"worker_{i}"
        if worker_name in workers and psutil.pid_exists(workers[worker_name]["pid"]):
            print(f"{worker_name} is already running (PID {workers[worker_name]['pid']}). Skipping.")
            continue

        print(f"Starting {worker_name}...")

        log_file = log_dir / "workers.log"

        with open(log_file, "a") as f:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m", "celery",
                    "-A", "apps.config",
                    "worker",
                    "--loglevel=info",
                    "-n", worker_name,
                ],
                stdout=f,
                stderr=subprocess.STDOUT,
            )

        workers[worker_name] = {"pid": process.pid, "log": str(log_file)}
        print(f"Started {worker_name} with PID {process.pid} (logging to {log_file})")

        save_workers(workers)  # Save after each successful start


def stop_worker(worker_name=None):
    workers = load_workers()

    if not workers:
        print("No workers found to stop.")
        return

    if worker_name is None:
        for name, info in list(workers.items()):
            pid = info.get("pid")
            terminate_worker(name, pid)
            del workers[name]
        save_workers(workers)
        return

    if worker_name not in workers:
        print(f"No worker named '{worker_name}' found.")
        return

    pid = workers[worker_name].get("pid")
    terminate_worker(worker_name, pid)

    del workers[worker_name]
    save_workers(workers)


def list_workers():
    workers = load_workers()
    if not workers:
        print("No workers recorded")
        return

    print("Registered Workers:\n")
    for name, info in workers.items():
        pid = info.get("pid")
        status = "running" if psutil.pid_exists(pid) else "not running"
        print(f"  - {name}: PID {pid} ({status})")

