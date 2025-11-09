from apps.jobs import JobRequest, submit_job, list_jobs, list_dlq, retry_dlq, job_status
from apps.workers import start_workers, stop_worker, list_workers
from apps.config import set_config
import argparse
import json

def main():
    parser = argparse.ArgumentParser(description="QueueCTL")
    sub = parser.add_subparsers(dest="cmd")

    j = sub.add_parser("job", help="Manage jobs")
    jsub = j.add_subparsers(dest="jcmd")

    # Enqueue new job
    jenq = jsub.add_parser("enqueue", help="Submit a new job")
    jenq.add_argument("--job", required=True)
    jenq.add_argument("--desc", default="")

    # List all known jobs
    jsub.add_parser("list", help="List all jobs")

    # Get status of a specific job
    jstatus = jsub.add_parser("status", help="Check job status")
    jstatus.add_argument("task_id")

    # DLQ management
    jsub.add_parser("dlq-list", help="List DLQ (Dead Letter Queue)")

    dlq_retry = jsub.add_parser("dlq-retry", help="Retry DLQ jobs")
    dlq_retry.add_argument("task_id", nargs="?", help="Retry a specific DLQ job by task_id")


    w = sub.add_parser("worker", help="Manage Celery workers")
    wsub = w.add_subparsers(dest="wcmd")

    # Start workers
    wstart = wsub.add_parser("start", help="Start one or more workers")
    wstart.add_argument("--count", type=int, default=1, help="Number of workers to start")

    # Stop workers
    wstop = wsub.add_parser("stop", help="Stop a worker or all workers")
    wstop.add_argument("--name", help="Name of worker to stop (omit to stop all)")

    # List workers
    wsub.add_parser("list", help="List all registered workers")

    # Config
    c = sub.add_parser("config", help="Manage configuration")
    csub = c.add_subparsers(dest="ccmd")

    cset = csub.add_parser("set", help="Set a configuration value")
    cset.add_argument("key", help="Config key")
    cset.add_argument("value", help="Config value")

    args = parser.parse_args()

    if args.cmd == "job":
        if args.jcmd == "enqueue":
            req = JobRequest(job_name=args.job, description=args.desc)
            print(json.dumps(submit_job(req), indent=2))

        elif args.jcmd == "list":
            print(json.dumps(list_jobs(), indent=2))

        elif args.jcmd == "status":
            print(json.dumps(job_status(args.task_id), indent=2))

        elif args.jcmd == "dlq-list":
            print(json.dumps(list_dlq(), indent=2))

        elif args.jcmd == "dlq-retry":
            print(json.dumps(retry_dlq(args.task_id), indent=2))

    elif args.cmd == "worker":
        if args.wcmd == "start":
            start_workers(args.count)

        elif args.wcmd == "stop":
            stop_worker(args.name)

        elif args.wcmd == "list":
            list_workers()

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
