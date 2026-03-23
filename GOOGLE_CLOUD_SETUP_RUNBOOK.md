# Google Cloud Setup Runbook

This is the practical checklist for taking the current cloud-send MVP live.

## Goal

After you finish a local run and it produces `final_send_queue.csv`, you can:

1. deploy that run to Google Cloud Storage
2. let a VM-side worker pick it up
3. wait until the target market's local send window
4. send through Gmail API without your local machine staying on

---

## Phase 1: Create Google Cloud resources

### 1. Create or choose a Google Cloud project

Use one dedicated project for outbound automation.

Example project id:

```text
solar-lead-intelligence-prod
```

### 2. Enable required APIs

Enable:

- Compute Engine API
- Cloud Storage API
- Secret Manager API
- IAM API

### 3. Create a GCS bucket

Example:

```bash
gcloud storage buckets create gs://solar-lead-runs-prod --location=us-central1
```

Use the same value later in:

- `GCS_BUCKET`

### 4. Create a VM

Recommended first VM:

- Ubuntu LTS
- 2 vCPU
- 4 GB RAM
- 30+ GB disk

Give the VM a service account with at least:

- Storage Object Admin on the chosen bucket
- Secret Manager Secret Accessor if you will fetch secrets from Secret Manager

---

## Phase 2: Prepare the VM

SSH into the VM and run:

```bash
export REPO_URL=https://github.com/<your-org>/<your-repo>.git
export REPO_BRANCH=main
bash deploy/gcp/bootstrap_vm.sh
```

What this does:

- installs Python and git
- optionally installs `gcloud`
- clones the repo
- creates `.venv`
- installs `requirements.txt`
- gives you the V2 maintenance scripts under `deploy/gcp/`

Then copy the example env:

```bash
cp deploy/gcp/.env.gcp.example .env
```

Fill at least:

- `GCS_BUCKET`
- sender identity
- your API keys if generation or status needs them

### Gmail OAuth files

Put these files on the VM under `config/`:

- `config/gmail_client_secret.json`
- `config/gmail_token.json`

If you prefer Secret Manager or a mounted secret directory, keep the same final
runtime file paths after restore. The VM now supports both:

- `SOLAR_SECRET_SOURCE_DIR`
- `SOLAR_GMAIL_CLIENT_SECRET_NAME`
- `SOLAR_GMAIL_TOKEN_SECRET_NAME`

Standard restore command:

```bash
bash deploy/gcp/restore_gmail_oauth.sh
```

---

## Phase 3: Install the worker service

Copy the service unit:

```bash
sudo cp deploy/gcp/systemd/cloud-send-worker.service /etc/systemd/system/cloud-send-worker.service
sudo systemctl daemon-reload
sudo systemctl enable cloud-send-worker
```

Before starting, make sure the repo path matches the service file:

- `/opt/solar-lead-intelligence`

Then start it:

```bash
sudo systemctl start cloud-send-worker
sudo systemctl status cloud-send-worker
```

Read logs with:

```bash
journalctl -u cloud-send-worker -f
```

The service now also performs a Gmail OAuth pre-start validation through:

- `deploy/gcp/restore_gmail_oauth.sh --check-only`

so missing token/client-secret problems fail fast at service start time.

---

## Phase 4: Deploy a completed run from local

On your local machine:

1. finish a run until `final_send_queue.csv` exists
2. set `GCS_BUCKET` in your local `.env`
3. authenticate `gcloud`

Then deploy one run:

```powershell
D:\Python\python.exe scripts\deploy_run_to_gcloud.py --campaign rio-de-janeiro_20260322_145618_296c
```

You can now also batch deploy:

```powershell
D:\Python\python.exe scripts\deploy_run_to_gcloud.py --campaign rio-... --campaign sao-...
```

Or auto-discover eligible completed runs:

```powershell
D:\Python\python.exe scripts\deploy_run_to_gcloud.py --all-ready --limit 5
```

What happens:

- the whole run folder goes to `gs://<bucket>/runs/<campaign_id>/`
- a manifest goes to `gs://<bucket>/manifests/<campaign_id>.json`
- already-deployed runs are skipped by default unless `--force` is passed

Manifest lifecycle on the VM worker side is now:

- success -> `processed/`
- failure -> `failed/`

---

## Phase 5: End-to-end validation

Use one Brazil run first.

Check:

1. the manifest appears in GCS
2. the VM worker downloads the run
3. the worker prints the next due send window
4. when the market-local window arrives, the worker runs:
   - Workflow 7 send
   - Workflow 8.5 status
5. updated run outputs are synced back to GCS

Files to confirm after send:

- `final_send_queue.csv`
- `send_batch_summary.json`
- `campaign_status.csv`
- `campaign_status_summary.json`
- `cloud_send_result.json`
- `cloud_send_status.json`

Also confirm deployment metadata after a VM update:

- `data/deploy_release.json`

---

## First live test recommendation

Do this in order:

1. test with one Brazil run
2. then one Saudi run
3. only then turn on normal usage for every completed run

For the first Gmail API live test, keep the queue small.

---

## Current code entrypoints

Local deploy:

- [deploy_run_to_gcloud.py](/d:/solar-lead-intelligence/scripts/deploy_run_to_gcloud.py)

Cloud worker:

- [cloud_send_worker.py](/d:/solar-lead-intelligence/scripts/cloud_send_worker.py)

VM bootstrap:

- [bootstrap_vm.sh](/d:/solar-lead-intelligence/deploy/gcp/bootstrap_vm.sh)

Service unit:

- [cloud-send-worker.service](/d:/solar-lead-intelligence/deploy/gcp/systemd/cloud-send-worker.service)

Cloud env template:

- [.env.gcp.example](/d:/solar-lead-intelligence/deploy/gcp/.env.gcp.example)

V2 update / recovery runbooks:

- [GITHUB_VM_UPDATE_RUNBOOK.md](/d:/solar-lead-intelligence/GITHUB_VM_UPDATE_RUNBOOK.md)
- [GMAIL_SECRET_AND_RECOVERY_RUNBOOK.md](/d:/solar-lead-intelligence/GMAIL_SECRET_AND_RECOVERY_RUNBOOK.md)

---

## V2 operator commands

Standard VM update:

```bash
bash deploy/gcp/update_vm.sh
```

Pinned release deploy / rollback:

```bash
bash deploy/gcp/update_vm.sh --ref <git-tag-or-commit>
```

or:

```bash
bash deploy/gcp/rollback_vm.sh <git-tag-or-commit>
```

Standard worker recovery:

```bash
bash deploy/gcp/recover_cloud_worker.sh
```

## Known first-version limits

- `deploy_run_to_gcloud.py` and `cloud_send_worker.py` still need one real GCS roundtrip validation
- Gmail OAuth restore is now standardized, but token issuance itself is still an external operator action
- the worker is single-process by design in V1
