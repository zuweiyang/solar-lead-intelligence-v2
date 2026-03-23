$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$PythonCandidates = @(
    "D:\Python\python.exe",
    "python",
    "py"
)

$PythonExe = $null
foreach ($candidate in $PythonCandidates) {
    try {
        if ($candidate -eq "py") {
            & $candidate -3 -c "print('ok')" *> $null
            $PythonExe = "$candidate -3"
            break
        }
        else {
            & $candidate -c "print('ok')" *> $null
            $PythonExe = $candidate
            break
        }
    }
    catch {
    }
}

if (-not $PythonExe) {
    throw "No usable Python interpreter found for GitHub bundle build."
}

Write-Host "[github-bundle] Using Python: $PythonExe"
Set-Location $ProjectRoot

$readinessArgs = @("scripts/check_github_readiness.py")
$exportArgs = @("scripts/prepare_github_repo_export.py", "--overwrite", "--zip")

if ($PythonExe -like "py *") {
    $parts = $PythonExe.Split(" ")
    & $parts[0] $parts[1] $readinessArgs
    & $parts[0] $parts[1] $exportArgs
}
else {
    & $PythonExe $readinessArgs
    & $PythonExe $exportArgs
}

Write-Host "[github-bundle] Done. Check _github_export and the generated .zip bundle."
