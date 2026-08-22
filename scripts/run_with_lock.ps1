#Requires -Version 5.1

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$CommandPath,
    [Parameter(Mandatory)]
    [string]$LogPath,
    [string]$LockName = "Global\JQuantsStockCollectorPipeline",
    [long]$MaxLogBytes = 10MB,
    [ValidateRange(1, 100)]
    [int]$Retention = 5
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$CommandPath = [IO.Path]::GetFullPath($CommandPath)
$LogPath = [IO.Path]::GetFullPath($LogPath)
if (-not (Test-Path -LiteralPath $CommandPath -PathType Leaf)) {
    throw "Command entry point is missing: $CommandPath"
}
if ($MaxLogBytes -lt 1) {
    throw "MaxLogBytes must be greater than zero."
}

function Rotate-OperationLog {
    if (-not (Test-Path -LiteralPath $LogPath -PathType Leaf)) {
        return
    }

    $log = Get-Item -LiteralPath $LogPath
    if ($log.Length -lt $MaxLogBytes) {
        return
    }

    $directory = $log.DirectoryName
    $baseName = [IO.Path]::GetFileNameWithoutExtension($log.Name)
    $extension = $log.Extension
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss-fff"
    $archivePath = Join-Path $directory "$baseName.$stamp$extension"
    Move-Item -LiteralPath $LogPath -Destination $archivePath

    Get-ChildItem -LiteralPath $directory -File -Filter "$baseName.*$extension" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -Skip $Retention |
        ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force }
}

$mutex = New-Object System.Threading.Mutex($false, $LockName)
$lockAcquired = $false
try {
    try {
        $lockAcquired = $mutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $lockAcquired = $true
        Write-Warning "Recovered an abandoned pipeline lock: $LockName"
    }

    if (-not $lockAcquired) {
        $message = "[SKIP] Pipeline lock is already held; command was not started: $CommandPath"
        Add-Content -LiteralPath $LogPath -Value $message -Encoding UTF8
        Write-Warning $message
        exit 75
    }

    Rotate-OperationLog

    $previousGuard = $env:JQUANTS_PIPELINE_LOCK_HELD
    try {
        $env:JQUANTS_PIPELINE_LOCK_HELD = "1"
        $escapedCommandPath = $CommandPath.Replace('"', '""')
        $commandLine = "call `"$escapedCommandPath`""
        & $env:ComSpec /d /s /c $commandLine
        $commandExitCode = $LASTEXITCODE
    }
    finally {
        $env:JQUANTS_PIPELINE_LOCK_HELD = $previousGuard
    }

    exit $commandExitCode
}
finally {
    if ($lockAcquired) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
}
