# Demo
https://github.com/user-attachments/assets/6050c5e2-8237-4ca5-a3cb-c828825893c8
# Quick Start

## 1. Initialize Environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment Configuration

Create a `.env` file in the project root (see configuration below).

Example env file:

```
MAX_RETRIES=3
BACKOFF_BASE=2
DEFAULT_RETRY_DELAY=5
RETRY_BACKOFF_BASE=2
MAX_RETRY_DELAY=3600
BROKER_URL=sqla+sqlite:///db.sqlite
BACKEND_URL=db+sqlite:///db.sqlite
```

---

## 2. Start Workers

Start one or more Celery workers:

```bash
python main.py worker start --count 2
```

List all running workers:

```bash
python main.py worker list
```

Stop all workers:

```bash
python main.py worker stop
```

Stop a specific worker:

```bash
python main.py worker stop worker_1
```

---

## 3. Submit Jobs

Queue a new job:

```bash
python main.py job enqueue --job mytask --desc "Run example task"
```

Simulate a failing job:

```bash
python main.py job enqueue --job fail --desc "This will trigger retries"
```

---

## 4. Monitor Jobs

List all jobs:

```bash
python main.py job list
```

List by state (for example “queued”, “completed”, “failed”):

```bash
python main.py job list --state failed
```

Check a specific job’s status:

```bash
python main.py job status <task_id>
```

---

## 5. Dead Letter Queue (DLQ)

List failed jobs:

```bash
python main.py job dlq-list
```

Retry a specific DLQ job:

```bash
python main.py job dlq-retry <task_id>
```

---

## 6. Configuration Management

View or change runtime retry behavior directly via CLI.

Set maximum retries to 5:

```bash
python main.py config set max_retries 5
```

Adjust backoff base (for exponential retry delays):

```bash
python main.py config set backoff_base 3
```

---

# Architecture Overview


| Component                     | Role                                                   |
| ----------------------------- | ------------------------------------------------------ |
| **Celery**                    | Manages background jobs and retries.                   |
| **SQLite (Broker + Backend)** | Stores messages, results, and job info.                |
| **JobStore (SQLite)**         | Persists job states and failed jobs (DLQ).             |
| **DLQ (Dead Letter Queue)**   | Keeps jobs that failed too many times.                 |
| **Workers**                   | Run jobs in the background.                            |

---

## Flow

``CLI input`` -> ``submit_job()`` -> ``Celery Broker (SQLite)`` -> ``Worker → process_job_task()`` -> ``JobStore (updates status)`` -> ``If fails → Retry (exponential backoff)`` -> ``If max retries reached`` → ``DLQ``


# My Learning Experience

I thought this was a great exercise. I hadn’t had the opportunity to implement workers and jobs and the like in of my other projects. While looking for the ideal aproach to queue management and the sort, I came to settle on celery, which is used in many production ready backends.


I decided to use sqlite for the backedn (yes, I know it’s not ideal to use an SQL database as a broker) because of its simplicity and ease of setup. Although spinning up a Redis Docker image would have been easy, I wanted to make the setup more convenient for evaluation.


There are a few limitations when using SQLite compared to brokers like redis or rabbitmq, where commands such as celery inspect and celery status don't work. However, I managed to overcome these issues by implementing a few workarounds.

Overall, this was an enjoyable exercise