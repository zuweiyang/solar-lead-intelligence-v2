# Gmail Secret And Recovery Runbook

This is the V2 runbook for Gmail OAuth placement, restoration, and worker recovery.

## Supported secret sources

The VM now supports two secret sources for Gmail OAuth files:

1. file-based restore directory via `SOLAR_SECRET_SOURCE_DIR`
2. Google Secret Manager via:
   - `SOLAR_GMAIL_CLIENT_SECRET_NAME`
   - `SOLAR_GMAIL_TOKEN_SECRET_NAME`

Final runtime file paths stay the same:

- `config/gmail_client_secret.json`
- `config/gmail_token.json`

Recommended fixed recovery source on the VM:

- set `SOLAR_SECRET_SOURCE_DIR` to an absolute path outside the repo
- example:
  - `/home/<vm-user>/solar-secrets/gmail`

---

## Recommended one-time hardening

After Gmail OAuth is working on the VM, stage the current files into the fixed
recovery directory:

```bash
mkdir -p /home/<vm-user>/solar-secrets/gmail
export SOLAR_SECRET_SOURCE_DIR=/home/<vm-user>/solar-secrets/gmail
bash deploy/gcp/stage_gmail_oauth.sh
```

This copies:

- `config/gmail_client_secret.json`
- `config/gmail_token.json`

into the stable restore source directory with `600` permissions.

That means future VM recovery can rely on one predictable path instead of
re-uploading OAuth files by hand.

---

## Secret Manager enablement

If you want to finish V2 on the fully managed secret path, publish the current
working Gmail OAuth files into Secret Manager:

```bash
export SOLAR_GMAIL_CLIENT_SECRET_NAME=solar-gmail-client-secret
export SOLAR_GMAIL_TOKEN_SECRET_NAME=solar-gmail-token
bash deploy/gcp/publish_gmail_oauth_to_secret_manager.sh
```

Then set the same secret names in `.env` and you can recover the worker without
depending on local staged files:

```bash
bash deploy/gcp/restore_gmail_oauth.sh --force
bash deploy/gcp/recover_cloud_worker.sh --skip-update
```

This path is now fully scripted; the remaining work is just applying it in your
live GCP project.

---

## Standard restore command

Run:

```bash
bash deploy/gcp/restore_gmail_oauth.sh
```

What it does:

- if runtime files already exist, it validates them
- if they are missing, it tries `SOLAR_SECRET_SOURCE_DIR`
- if file-source restore is not available, it tries Secret Manager
- it writes the runtime files back into `config/`
- it sets file permissions to `600`

---

## Validation-only check

The systemd worker now runs a pre-start validation:

```bash
bash deploy/gcp/restore_gmail_oauth.sh --check-only
```

This gives a clear startup failure if Gmail OAuth files are missing, instead of a
later ambiguous Gmail API error.

---

## VM rebuild path

After rebuilding a VM:

1. clone the repo and bootstrap it
2. recreate `.env`
3. set either:
   - `SOLAR_SECRET_SOURCE_DIR`
   - or `SOLAR_GMAIL_CLIENT_SECRET_NAME` and `SOLAR_GMAIL_TOKEN_SECRET_NAME`
4. run:

```bash
bash deploy/gcp/restore_gmail_oauth.sh
```

5. start or recover the worker:

```bash
bash deploy/gcp/recover_cloud_worker.sh --skip-update
```

---

## If Gmail token expires or goes invalid

Expected operator path:

1. generate or replace the token out of band
2. update the file source or Secret Manager secret value
3. run:

```bash
bash deploy/gcp/restore_gmail_oauth.sh --force
bash deploy/gcp/recover_cloud_worker.sh --skip-update
```

This makes token replacement explicit and repeatable.

If you use the fixed recovery directory approach, the token refresh path becomes:

1. replace the staged token in `SOLAR_SECRET_SOURCE_DIR`
2. run:

```bash
bash deploy/gcp/restore_gmail_oauth.sh --force
bash deploy/gcp/recover_cloud_worker.sh --skip-update
```

---

## Related files

- [publish_gmail_oauth_to_secret_manager.sh](/d:/solar-lead-intelligence/deploy/gcp/publish_gmail_oauth_to_secret_manager.sh)
- [stage_gmail_oauth.sh](/d:/solar-lead-intelligence/deploy/gcp/stage_gmail_oauth.sh)
- [restore_gmail_oauth.sh](/d:/solar-lead-intelligence/deploy/gcp/restore_gmail_oauth.sh)
- [recover_cloud_worker.sh](/d:/solar-lead-intelligence/deploy/gcp/recover_cloud_worker.sh)
- [.env.gcp.example](/d:/solar-lead-intelligence/deploy/gcp/.env.gcp.example)
- [cloud-send-worker.service](/d:/solar-lead-intelligence/deploy/gcp/systemd/cloud-send-worker.service)
