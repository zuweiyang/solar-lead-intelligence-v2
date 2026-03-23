# GitHub-Ready Checklist

Use this before uploading the project to GitHub.

## Current recommendation

Use a **private repository** first.

This project currently works with:

- Gmail OAuth files
- API keys in `.env`
- real lead/contact/send data under `data/`

Those must not be uploaded.

## Safe to upload

- source code under `src/`
- scripts under `scripts/`
- tests under `tests/`
- docs and runbooks
- config code such as [settings.py](/d:/solar-lead-intelligence/config/settings.py)
- templates:
  - [.env.example](/d:/solar-lead-intelligence/.env.example)
  - [.env.gcp.example](/d:/solar-lead-intelligence/deploy/gcp/.env.gcp.example)
  - cloud auto-deploy stays opt-in through `CLOUD_AUTO_DEPLOY_ON_COMPLETE`

## Do not upload

- [.env](/d:/solar-lead-intelligence/.env)
- [gmail_client_secret.json](/d:/solar-lead-intelligence/config/gmail_client_secret.json)
- [gmail_token.json](/d:/solar-lead-intelligence/config/gmail_token.json)
- the whole [data](/d:/solar-lead-intelligence/data) directory
- [node_modules](/d:/solar-lead-intelligence/node_modules)
- local caches such as `.claude` and `.tldextract-cache`

## Before first upload

1. Confirm [.gitignore](/d:/solar-lead-intelligence/.gitignore) includes secrets, runtime data, and local caches.
2. Keep the first repo **private**.
3. Run the local readiness check:

```powershell
D:\Python\python.exe scripts\check_github_readiness.py
```

4. Build a clean GitHub export instead of uploading the raw workspace:

```powershell
D:\Python\python.exe scripts\prepare_github_repo_export.py --overwrite
```

Or use the new one-click helper:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\build_github_bundle.ps1
```

This will:

- run the readiness check
- build `_github_export`
- also create a zip bundle automatically

5. Make sure you upload templates instead of secrets:
   - `.env.example`
   - `deploy/gcp/.env.gcp.example`
6. Do not copy runtime `data/` into the repository. Keep only `data/.gitkeep`.

## Important limitation

`.gitignore` protects future commits. It does **not** protect files that were
already committed in git history.

If a secret is ever committed, rotate it:

- Gmail OAuth client secret
- Gmail token
- API keys

## Suggested next step

After the repository is cleaned and made private, use GitHub as the code source
for the GCP VM, while continuing to send run artifacts through GCS.

For the standardized V2 VM update flow, see:

- [GITHUB_VM_UPDATE_RUNBOOK.md](/d:/solar-lead-intelligence/GITHUB_VM_UPDATE_RUNBOOK.md)
- [GMAIL_SECRET_AND_RECOVERY_RUNBOOK.md](/d:/solar-lead-intelligence/GMAIL_SECRET_AND_RECOVERY_RUNBOOK.md)

See also:

- [GITHUB_EXPORT_STRUCTURE.md](/d:/solar-lead-intelligence/GITHUB_EXPORT_STRUCTURE.md)
