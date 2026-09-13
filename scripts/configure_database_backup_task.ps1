#Requires -Version 5.1
#Requires -RunAsAdministrator

[CmdletBinding()]
param(
    [string]$RepositoryRoot,
    [string]$BackupDirectory,
    [string]$ConfigurationBackupDirectory,
    [string]$ResultPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($RepositoryRoot)) {
    $RepositoryRoot = Split-Path -Parent $PSScriptRoot
}
$RepositoryRoot = [IO.Path]::GetFullPath($RepositoryRoot)

if ([string]::IsNullOrWhiteSpace($BackupDirectory)) {
    $documents = [Environment]::GetFolderPath("MyDocuments")
    $BackupDirectory = Join-Path $documents "Codex Backups\jquants-stock-collector\database"
}
$BackupDirectory = [IO.Path]::GetFullPath($BackupDirectory)

if ([string]::IsNullOrWhiteSpace($ConfigurationBackupDirectory)) {
    $documents = [Environment]::GetFolderPath("MyDocuments")
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $ConfigurationBackupDirectory = Join-Path $documents (
        "Codex Backups\jquants-stock-collector\task-scheduler-database-backup-$stamp"
    )
}
if (-not [string]::IsNullOrWhiteSpace($ResultPath)) {
    $ResultPath = [IO.Path]::GetFullPath($ResultPath)
    if (Test-Path -LiteralPath $ResultPath) {
        throw "ResultPath already exists; refusing to overwrite it: $ResultPath"
    }
}

$taskName = "NISA-JQuant Database Backup"
$runner = Join-Path $RepositoryRoot "scripts\run_database_backup.ps1"
if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) {
    throw "Database backup runner is missing: $runner"
}

$powerShellPath = Join-Path $PSHOME "powershell.exe"
$arguments = (
    '-NoProfile -ExecutionPolicy Bypass -File "{0}" -RepositoryRoot "{1}" ' +
    '-BackupDirectory "{2}" -RetentionCount 8 -MinimumCount 1 ' +
    '-MaxTotalBytes 21474836480'
) -f $runner, $RepositoryRoot, $BackupDirectory
$workingDirectory = $RepositoryRoot.TrimEnd('\') + '\'
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
$existingXml = $null
$created = $false
$changed = $false

if ($null -ne $existing) {
    $existingArguments = [string]$existing.Actions[0].Arguments
    if (
        $existing.Actions.Count -ne 1 -or
        [string]$existing.Actions[0].Execute -ne $powerShellPath -or
        -not $existingArguments.Contains($runner)
    ) {
        throw "Unexpected action on '$taskName'; refusing to modify it."
    }
    New-Item -ItemType Directory -Path $ConfigurationBackupDirectory -Force |
        Out-Null
    $existingXml = Export-ScheduledTask -TaskName $taskName
    Set-Content -LiteralPath (
        Join-Path $ConfigurationBackupDirectory "database-backup-task.xml"
    ) -Value $existingXml -Encoding Unicode
}

try {
    $action = New-ScheduledTaskAction `
        -Execute $powerShellPath `
        -Argument $arguments `
        -WorkingDirectory $workingDirectory
    $trigger = New-ScheduledTaskTrigger `
        -Weekly `
        -WeeksInterval 1 `
        -DaysOfWeek Saturday `
        -At 9am
    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -MultipleInstances IgnoreNew `
        -ExecutionTimeLimit (New-TimeSpan -Hours 2)
    $userId = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    $principal = New-ScheduledTaskPrincipal `
        -UserId $userId `
        -LogonType Interactive `
        -RunLevel Highest
    $definition = New-ScheduledTask `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description "Weekly verified SQLite backup; keep 8 generations within 20 GiB."

    Register-ScheduledTask `
        -TaskName $taskName `
        -InputObject $definition `
        -Force | Out-Null
    $changed = $true
    $created = $null -eq $existing

    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $taskName -ErrorAction Stop
    if ($task.Actions.Count -ne 1) {
        throw "Backup task action count verification failed."
    }
    if ([string]$task.Actions[0].Execute -ne $powerShellPath) {
        throw "Backup task executable verification failed."
    }
    if (-not ([string]$task.Actions[0].Arguments).Contains($runner)) {
        throw "Backup task runner verification failed."
    }
    $taskXml = [xml](Export-ScheduledTask -TaskName $taskName)
    $namespace = New-Object System.Xml.XmlNamespaceManager($taskXml.NameTable)
    $namespace.AddNamespace(
        "t",
        "http://schemas.microsoft.com/windows/2004/02/mit/task"
    )
    $calendar = $taskXml.SelectSingleNode("//t:CalendarTrigger", $namespace)
    $saturday = $taskXml.SelectSingleNode(
        "//t:CalendarTrigger/t:ScheduleByWeek/t:DaysOfWeek/t:Saturday",
        $namespace
    )
    if ($null -eq $calendar -or $null -eq $saturday) {
        throw "Backup task weekly Saturday trigger verification failed."
    }
    if (([datetime]$calendar.StartBoundary).TimeOfDay -ne [timespan]::FromHours(9)) {
        throw "Backup task 09:00 trigger verification failed."
    }
    if (-not [bool]$task.Settings.StartWhenAvailable) {
        throw "Backup task StartWhenAvailable verification failed."
    }
    if ([string]$task.Settings.MultipleInstances -ne "IgnoreNew") {
        throw "Backup task MultipleInstances verification failed."
    }
    $result = [pscustomobject]@{
        Succeeded = $true
        WorkflowsExecuted = $false
        TaskName = $taskName
        State = [string]$task.State
        Execute = [string]$task.Actions[0].Execute
        Arguments = [string]$task.Actions[0].Arguments
        WorkingDirectory = [string]$task.Actions[0].WorkingDirectory
        NextRunTime = $info.NextRunTime.ToString("o")
        LastTaskResult = $info.LastTaskResult
        NumberOfMissedRuns = $info.NumberOfMissedRuns
        DaysOfWeek = @("Saturday")
        StartTime = "09:00:00"
        StartWhenAvailable = [bool]$task.Settings.StartWhenAvailable
        MultipleInstances = [string]$task.Settings.MultipleInstances
        RetentionCount = 8
        MinimumCount = 1
        MaxTotalBytes = 21474836480
    } | ConvertTo-Json -Depth 4
    if (-not [string]::IsNullOrWhiteSpace($ResultPath)) {
        $resultParent = Split-Path -Parent $ResultPath
        if (-not [string]::IsNullOrWhiteSpace($resultParent)) {
            New-Item -ItemType Directory -Path $resultParent -Force | Out-Null
        }
        Set-Content -LiteralPath $ResultPath -Value $result -Encoding UTF8
    }
    $result
}
catch {
    $failure = $_.Exception.Message
    try {
        if ($changed -and $null -ne $existingXml) {
            Register-ScheduledTask `
                -TaskName $taskName `
                -Xml $existingXml `
                -Force | Out-Null
        }
        elseif ($changed -and $created) {
            Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
        }
    }
    catch {
        throw "Backup task configuration failed: $failure Rollback also failed: $($_.Exception.Message)"
    }
    throw "Backup task configuration failed and was rolled back: $failure"
}
