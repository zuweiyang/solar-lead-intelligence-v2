# Google Cloud Minimal Deployment Plan

## Goal

After a run finishes locally, its artifacts should be pushed to Google Cloud.
Google Cloud should then wait until the target market's local send window and
run Gmail API sending automatically, without depending on the local machine.

This document describes the minimum viable long-term architecture for the
current codebase.

---

## Recommended First Version

Use:

- Google Compute Engine VM
- Google Secret Manager
- Google Cloud Storage
- a small deploy hook from local -> cloud
- a long-running worker on the VM

Why this is the best first version for the current repo:

- the project is file-driven (`data/runs/<campaign_id>/...`)
- send timing is computed inside Python, not by an external workflow engine
- Gmail API token handling is easier on a persistent VM than on Cloud Run
- the repo already has local scheduler / watcher patterns that map naturally to a VM worker

---

## Topology

```text
Local Machine
  |
  | 1. run completes locally
  | 2. deploy_run_to_gcloud.py uploads run folder + manifest
  v
Google Cloud Storage (optional but recommended)
  - incoming run bundle
  - deployment manifest
  - backups / archival artifacts
  |
  v
Google Compute Engine VM
  - repo checkout
  - Python runtime
  - Gmail API dependencies
  - long-running cloud_send_worker.py
  - local workspace copy of active runs
  |
  +--> Google Secret Manager
  |     - Gmail client secret
  |     - Gmail token / refreshable auth material
  |     - OpenRouter / Anthropic / OpenAI keys
  |     - any SMTP fallback secrets if retained
  |
  +--> Gmail API
  |     - send real emails at target-market local time
  |
  +--> optional future:
        - Cloud Logging
        - Monitoring / alerting
        - Cloud Scheduler heartbeat
```

---

## Runtime Model

### Local side

The local machine remains responsible for:

- search
- scrape
- crawl
- classify
- score
- enrich
- email generation
- repair
- producing `final_send_queue.csv`

Once a run is ready, local code should trigger:

- upload of `data/runs/<campaign_id>/`
- creation of a small manifest saying:
  - campaign id
  - ready for cloud send
  - send mode = `gmail_api`
  - run status = `queued_for_cloud_send`

### Cloud side

The VM worker should:

1. poll for new deployed runs
2. copy or unpack them into the VM workspace
3. inspect `final_send_queue.csv`
4. compute next eligible send time from target-market local time
5. sleep / wait until due
6. run Workflow 7 send in `gmail_api` mode
7. run Workflow 8.5 status refresh
8. write a send result marker
9. optionally sync summaries back to GCS

---

## First-Version Data Flow

```text
Local run completed
  -> deploy script uploads run folder
  -> cloud worker detects new campaign_id
  -> cloud worker watches next eligible window
  -> send_pipeline.run(campaign_id=..., send_mode="gmail_api")
  -> status_pipeline.run(campaign_id=...)
  -> write cloud_send_result.json
```

---

## What To Build First

### Phase 1: Infrastructure

Provision:

- one Compute Engine VM
- one GCS bucket
- one Secret Manager namespace

Recommended first VM shape:

- small Linux VM
- 2 vCPU / 4 GB RAM class is enough for send-worker duties
- persistent disk large enough for repo + run artifacts

Why Linux VM first:

- easier background service management with `systemd`
- easier long-running workers
- lower operational friction than Windows VM

### Phase 2: Cloud workspace bootstrap

On the VM:

- clone repo
- install Python dependencies
- store runtime files under a stable workspace path
- configure env loading
- verify Gmail API auth works

### Phase 3: Local deploy hook

Create a local script:

- `scripts/deploy_run_to_gcloud.py`

Responsibilities:

- validate the target run exists
- verify `final_send_queue.csv` exists
- upload run folder to GCS or direct to VM
- register a deploy manifest for the worker

### Phase 4: Cloud send worker

Create a VM worker:

- `scripts/cloud_send_worker.py`

Responsibilities:

- poll for newly deployed runs
- avoid duplicate processing
- calculate next send window from `send_guard.next_eligible_send_time()`
- trigger:
  - `send_pipeline.run(...)`
  - `status_pipeline.run(...)`
- persist worker state

### Phase 5: Service management

Run the worker as a background service:

- `systemd` unit on Linux VM

This makes cloud sending survive:

- SSH session closure
- terminal closure
- VM reboot

---

## First-Version Deployment Steps

### Step 1

Create a Google Cloud project for outbound automation.

### Step 2

Enable APIs:

- Compute Engine API
- Secret Manager API
- Cloud Storage API
- IAM API

### Step 3

Create:

- one GCS bucket, for example `solar-lead-runs-prod`
- one VM, for example `solar-send-worker-1`

### Step 4

Install on VM:

- Python
- git
- project dependencies from `requirements.txt`

### Step 5

Move secrets into Secret Manager:

- Gmail OAuth client secret
- Gmail token material
- `.env` secrets currently required for generation / scoring / sending

### Step 6

Create a VM bootstrap script that:

- fetches secrets from Secret Manager
- writes a runtime `.env`
- starts the worker

### Step 7

Implement `deploy_run_to_gcloud.py` locally.

### Step 8

Implement `cloud_send_worker.py` on the VM.

### Step 9

Test with one completed Brazil run:

- upload
- wait until computed send window
- send one or two emails first
- verify `send_logs` and `campaign_status_summary`

### Step 10

Expand to normal operation:

- local run finishes
- deploy script triggers automatically or manually
- cloud worker owns send timing

---

## Suggested MVP Operational Rules

- local machine owns research and queue construction
- cloud owns all real Gmail sending
- each run is immutable once deployed
- if a run is redeployed, use a new deploy version marker
- worker must be idempotent:
  - never send the same run twice accidentally

---

## Existing Code To Reuse

These pieces are already good building blocks:

- `scripts/auto_send_runs.py`
  - already computes target-market send timing
  - can be adapted into the VM worker core

- `src/workflow_7_email_sending/send_guard.py`
  - already knows target-market local windows
  - already exposes `next_eligible_send_time()`

- `src/workflow_7_email_sending/send_pipeline.py`
  - already runs actual send logic

- `src/workflow_8_5_campaign_status/status_pipeline.py`
  - already rebuilds post-send status

- `config/run_context.py`
  - already supports run-scoped execution

---

## Files That Should Change

### New files

- `scripts/deploy_run_to_gcloud.py`
  - local deploy hook

- `scripts/cloud_send_worker.py`
  - VM-side long-running watcher / sender

- `deploy/gcp/systemd/cloud-send-worker.service`
  - Linux service definition

- `deploy/gcp/bootstrap_vm.sh`
  - VM bootstrap / setup script

- `deploy/gcp/sync_run_from_gcs.py`
  - optional helper if using GCS as the transport layer

### Existing files likely to change

- `src/workflow_7_email_sending/send_logger.py`
  - add optional fields like `execution_host`, `execution_environment`, `cloud_deploy_id`

- `src/workflow_9_campaign_runner/campaign_runner.py`
  - optional post-run hook: auto-deploy completed runs

- `src/workflow_9_campaign_runner/campaign_steps.py`
  - optional integration point if deploy is triggered at the end of `campaign_status`

- `config/settings.py`
  - cloud-specific settings:
    - `CLOUD_SEND_ENABLED`
    - `GCS_BUCKET`
    - `CLOUD_ENVIRONMENT`
    - `CLOUD_WORKER_POLL_SECONDS`
    - `SECRET_MANAGER_ENABLED`

- `PROJECT_CONTEXT.md`
  - update once the cloud worker path is implemented

### Files that probably do not need major redesign

- `src/workflow_7_email_sending/send_guard.py`
- `src/workflow_7_email_sending/send_pipeline.py`
- `src/workflow_8_5_campaign_status/status_pipeline.py`

These should mostly be reused, not replaced.

---

## Minimal Cloud Worker Behavior

Pseudo-flow:

```python
while True:
    deployed_runs = list_pending_cloud_runs()
    for campaign_id in deployed_runs:
        rows = load_final_send_queue(campaign_id)
        due = min(next_eligible_send_time(row, campaign_id=campaign_id) for row in rows)
        if now_utc >= due:
            set_active_run(campaign_id)
            send_pipeline.run(campaign_id=campaign_id, send_mode="gmail_api")
            status_pipeline.run(campaign_id=campaign_id)
            mark_cloud_run_sent(campaign_id)
            clear_active_run()
    sleep(poll_interval)
```

---

## Security Notes

- do not leave Gmail client secrets only on the local machine
- do not commit token files into git
- use Secret Manager for production secrets
- restrict VM service account permissions to only:
  - Secret Manager access
  - GCS bucket access
  - optional logging permissions

---

## What The First Version Should Not Try To Solve

Avoid overbuilding on day one:

- no need for Pub/Sub first
- no need for Cloud Run first
- no need for multi-worker fanout first
- no need for autoscaling first
- no need for full database migration first

The first target is simple:

- run is ready locally
- run is shipped to cloud
- cloud waits for market-local send time
- cloud sends

---

## Recommended Order Of Work

1. add deploy hook
2. add cloud worker
3. add VM bootstrap
4. move secrets to Secret Manager
5. test with one Brazil run
6. test with one Saudi run
7. only then add better monitoring / retries

---

## Expected Build Time

Reasonable first-version estimate:

- MVP: 1 to 2 days
- more production-safe version: 2 to 4 days

---

## Definition Of Done For V1

V1 is done when all of these are true:

- a completed local run can be deployed to GCP
- deployed runs appear on the VM automatically
- VM waits for target-market local send window
- VM sends via Gmail API without local-machine involvement
- send and status outputs are persisted after cloud execution
- local machine can be off and the send still happens
