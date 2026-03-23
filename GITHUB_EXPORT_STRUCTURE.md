# GitHub Export Structure

This is the shape of the repository you should upload to GitHub.

## Keep in the repository

- [src](/d:/solar-lead-intelligence/src)
- [scripts](/d:/solar-lead-intelligence/scripts)
- [tests](/d:/solar-lead-intelligence/tests)
- [config](/d:/solar-lead-intelligence/config)
  - keep code files
  - do not include Gmail OAuth JSON files
- [deploy](/d:/solar-lead-intelligence/deploy)
- docs and runbooks in the project root
- [requirements.txt](/d:/solar-lead-intelligence/requirements.txt)
- [package.json](/d:/solar-lead-intelligence/package.json)
- [package-lock.json](/d:/solar-lead-intelligence/package-lock.json)
- [.gitignore](/d:/solar-lead-intelligence/.gitignore)
- [.env.example](/d:/solar-lead-intelligence/.env.example)

## Keep as placeholders only

- [data](/d:/solar-lead-intelligence/data)
  - include only [data/.gitkeep](/d:/solar-lead-intelligence/data/.gitkeep)

## Do not upload

- [data/runs](/d:/solar-lead-intelligence/data/runs)
- [data/crm](/d:/solar-lead-intelligence/data/crm)
- [gmail_client_secret.json](/d:/solar-lead-intelligence/config/gmail_client_secret.json)
- [gmail_token.json](/d:/solar-lead-intelligence/config/gmail_token.json)
- [node_modules](/d:/solar-lead-intelligence/node_modules)
- [`.env`](/d:/solar-lead-intelligence/.env)
- local caches and logs

## Recommended workflow

1. Run the readiness check:

```powershell
D:\Python\python.exe scripts\check_github_readiness.py
```

2. Build a clean export:

```powershell
D:\Python\python.exe scripts\prepare_github_repo_export.py --overwrite
```

Or one-click:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\build_github_bundle.ps1
```

3. Upload the export directory, not the raw working directory.
4. Use GitHub as the VM code source and update the VM with:

```bash
bash deploy/gcp/update_vm.sh
```

## Why this is safer

Your working directory contains real leads, contact data, send logs, and OAuth
material. The export directory keeps only code and templates, so you are much
less likely to accidentally push runtime data to GitHub.
