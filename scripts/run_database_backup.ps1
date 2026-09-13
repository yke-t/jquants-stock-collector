#Requires -Version 5.1

[CmdletBinding()]
param(
    [string]$RepositoryRoot,
    [string]$SourcePath,
    [string]$BackupDirectory,
    [ValidateRange(1, 100)]
    [int]$RetentionCount = 8,
    [ValidateRange(1, 100)]
    [int]$MinimumCount = 1,
    [long]$MaxTotalBytes = 20GB,
    [string]$LockName = "Global\JQuantsStockCollectorPipeline"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($RepositoryRoot)) {
    $RepositoryRoot = Split-Path -Parent $PSScriptRoot
}
$RepositoryRoot = [IO.Path]::GetFullPath($RepositoryRoot)

if ([string]::IsNullOrWhiteSpace($SourcePath)) {
    $SourcePath = Join-Path $RepositoryRoot "stock_data.db"
}
$SourcePath = [IO.Path]::GetFullPath($SourcePath)

if ([string]::IsNullOrWhiteSpace($BackupDirectory)) {
    $documents = [Environment]::GetFolderPath("MyDocuments")
    $BackupDirectory = Join-Path $documents "Codex Backups\jquants-stock-collector\database"
}
$BackupDirectory = [IO.Path]::GetFullPath($BackupDirectory)

$backupScript = Join-Path $RepositoryRoot "scripts\backup_database.py"
$retentionScript = Join-Path $RepositoryRoot "scripts\backup_retention.py"
foreach ($path in @($SourcePath, $backupScript, $retentionScript)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Required file is missing: $path"
    }
}
if ($RetentionCount -lt $MinimumCount) {
    throw "RetentionCount must be at least MinimumCount."
}
if ($MaxTotalBytes -lt 1) {
    throw "MaxTotalBytes must be greater than zero."
}

$backupDirectoryPrefix = $BackupDirectory.TrimEnd('\') + '\'
$repositoryPrefix = $RepositoryRoot.TrimEnd('\') + '\'
if ($BackupDirectory -eq $RepositoryRoot -or $backupDirectoryPrefix.StartsWith(
    $repositoryPrefix,
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "BackupDirectory must be outside RepositoryRoot."
}

New-Item -ItemType Directory -Path $BackupDirectory -Force | Out-Null
$logPath = Join-Path $BackupDirectory "backup_operation.log"

function Write-BackupLog {
    param([Parameter(Mandatory)][string]$Message)
    $line = "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ss.fffK') $Message"
    Add-Content -LiteralPath $logPath -Value $line -Encoding UTF8
    Write-Output $line
}

function Rotate-BackupLog {
    if (-not (Test-Path -LiteralPath $logPath -PathType Leaf)) {
        return
    }
    $log = Get-Item -LiteralPath $logPath
    if ($log.Length -lt 2MB) {
        return
    }
    $archive = Join-Path $BackupDirectory (
        "backup_operation.{0}.log" -f (Get-Date -Format "yyyyMMdd-HHmmss-fff")
    )
    Move-Item -LiteralPath $logPath -Destination $archive
    Get-ChildItem -LiteralPath $BackupDirectory -File -Filter "backup_operation.*.log" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -Skip 4 |
        ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force }
}

$pythonPath = Join-Path $RepositoryRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
    $pythonCommand = Get-Command python -ErrorAction Stop
    $pythonPath = $pythonCommand.Source
}

$mutex = New-Object System.Threading.Mutex($false, $LockName)
$lockAcquired = $false
try {
    try {
        $lockAcquired = $mutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $lockAcquired = $true
        Write-BackupLog "[WARN] Recovered abandoned mutex: $LockName"
    }
    if (-not $lockAcquired) {
        Write-BackupLog "[SKIP] Pipeline lock is held; backup was not started."
        exit 75
    }

    Rotate-BackupLog
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $backupPath = Join-Path $BackupDirectory "stock_data-$stamp.db"
    $resultPath = Join-Path $BackupDirectory "stock_data-$stamp.verification.json"
    Write-BackupLog "[START] source=$SourcePath backup=$backupPath"

    $backupArguments = @(
        $backupScript,
        "--source", $SourcePath,
        "--output", $backupPath,
        "--result", $resultPath,
        "--restore-drill"
    )
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $pythonPath @backupArguments 2>&1 |
            ForEach-Object {
                if ($_ -is [Management.Automation.ErrorRecord]) {
                    $_.Exception.Message
                }
                else {
                    [string]$_
                }
            } |
            Tee-Object -FilePath $logPath -Append
        $backupExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($backupExitCode -ne 0) {
        Write-BackupLog "[ERROR] Backup verification failed: exit=$backupExitCode"
        exit $backupExitCode
    }

    $retentionArguments = @(
        $retentionScript,
        "--directory", $BackupDirectory,
        "--retain-count", $RetentionCount,
        "--minimum-count", $MinimumCount,
        "--max-total-bytes", $MaxTotalBytes,
        "--apply"
    )
    try {
        $ErrorActionPreference = "Continue"
        & $pythonPath @retentionArguments 2>&1 |
            ForEach-Object {
                if ($_ -is [Management.Automation.ErrorRecord]) {
                    $_.Exception.Message
                }
                else {
                    [string]$_
                }
            } |
            Tee-Object -FilePath $logPath -Append
        $retentionExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($retentionExitCode -ne 0) {
        Write-BackupLog "[ERROR] Retention failed: exit=$retentionExitCode"
        exit $retentionExitCode
    }

    Write-BackupLog "[END] Verified backup and retention completed."
    exit 0
}
finally {
    if ($lockAcquired) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
}
