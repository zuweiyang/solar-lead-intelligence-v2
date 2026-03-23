# GitHub To VM Update Runbook

This is the V2 update path for making GitHub the source of truth and keeping the
VM on a repeatable release flow.

## Goal

Use one standard sequence:

1. local changes land in GitHub
2. VM pulls the exact branch from GitHub
3. dependencies refresh
4. worker service restarts cleanly
5. deployed revision is written to `data/deploy_release.json`

---

## Source of truth

- GitHub repository = canonical code source
- GCS bucket = runtime run-artifact transport
- VM local checkout = disposable deployment copy, not the primary authoring copy

Do not manually copy single files to the VM in V2.

---

## First VM setup

Bootstrap once:

```bash
export REPO_URL=https://github.com/<your-org>/<your-repo>.git
export REPO_BRANCH=main
bash deploy/gcp/bootstrap_vm.sh
```

Then create the VM env file:

```bash
cp deploy/gcp/.env.gcp.example .env
```

---

## Standard update flow

After code is pushed to GitHub, update the VM with:

```bash
bash deploy/gcp/update_vm.sh
```

What it does:

- `git fetch --tags --prune`
- checks out the configured branch
- `git pull --ff-only`
- refreshes `.venv` and `requirements.txt`
- reinstalls the systemd unit
- restarts `cloud-send-worker`
- writes deployment metadata to:
  - `data/deploy_release.json`

This file gives you a quick answer to:

- which branch is deployed
- which commit is deployed
- when it was updated
- whether the VM worktree is dirty

You can also pin deployment to an exact tag or commit:

```bash
bash deploy/gcp/update_vm.sh --ref v2.0.0
```

or:

```bash
bash deploy/gcp/update_vm.sh --ref a1b2c3d
```

---

## Recovery flow

If the worker stops, secrets go missing, or the VM restarts into a bad state:

```bash
bash deploy/gcp/recover_cloud_worker.sh
```

What it does:

- runs the normal update flow
- restores or validates Gmail OAuth files
- restarts the worker service
- prints systemd status
- prints recent worker logs
- prints recent structured worker alerts if present

---

## Rollback flow

If a fresh deployment needs to be reverted:

```bash
bash deploy/gcp/rollback_vm.sh <git-tag-or-commit>
```

This reuses the same update path but pins the VM to the exact requested ref.

Recommended rollback target:

- a Git tag
- or a previously known-good commit SHA

To inspect the currently deployed release metadata:

```bash
bash deploy/gcp/release_status.sh
```

---

## Release hygiene

Recommended operator habit:

1. make changes locally
2. run your local checks
3. push to GitHub
4. SSH to the VM
5. run `bash deploy/gcp/update_vm.sh`
6. confirm `systemctl status cloud-send-worker`
7. confirm `data/deploy_release.json`
8. if needed, use `bash deploy/gcp/rollback_vm.sh <tag-or-commit>`

---

## Related files

- [bootstrap_vm.sh](/d:/solar-lead-intelligence/deploy/gcp/bootstrap_vm.sh)
- [update_vm.sh](/d:/solar-lead-intelligence/deploy/gcp/update_vm.sh)
- [rollback_vm.sh](/d:/solar-lead-intelligence/deploy/gcp/rollback_vm.sh)
- [release_status.sh](/d:/solar-lead-intelligence/deploy/gcp/release_status.sh)
- [recover_cloud_worker.sh](/d:/solar-lead-intelligence/deploy/gcp/recover_cloud_worker.sh)
- [cloud-send-worker.service](/d:/solar-lead-intelligence/deploy/gcp/systemd/cloud-send-worker.service)
- [GOOGLE_CLOUD_SETUP_RUNBOOK.md](/d:/solar-lead-intelligence/GOOGLE_CLOUD_SETUP_RUNBOOK.md)
