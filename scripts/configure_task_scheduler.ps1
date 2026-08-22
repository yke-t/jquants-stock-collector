#Requires -Version 5.1
#Requires -RunAsAdministrator

[CmdletBinding()]
param(
    [string]$RepositoryRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$BackupDirectory,
    [string]$ResultPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$dailyTaskName = "NISA-JQuant Daily"
$dividendTaskName = "NISA-JQuant Dividend Daily"
$monthlyTaskName = "SnowMoney_Monthly_Eval"
$dailyBatch = Join-Path $RepositoryRoot "run_daily.bat"
$dividendBatch = Join-Path $RepositoryRoot "run_dividend_daily.bat"
$workingDirectory = "$RepositoryRoot\"
$weekdays = @("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")

if ([string]::IsNullOrWhiteSpace($BackupDirectory)) {
    $documents = [Environment]::GetFolderPath("MyDocuments")
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $BackupDirectory = Join-Path $documents "Codex Backups\jquants-stock-collector\task-scheduler-$stamp"
}

foreach ($path in @($dailyBatch, $dividendBatch)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Required batch entry point is missing: $path"
    }
}

function Export-TaskBackup {
    param(
        [Parameter(Mandatory)]
        [string]$TaskName,
        [Parameter(Mandatory)]
        [string]$FileName
    )

    $xml = Export-ScheduledTask -TaskName $TaskName
    $target = Join-Path $BackupDirectory $FileName
    Set-Content -LiteralPath $target -Value $xml -Encoding Unicode
    return $xml
}

function Get-TaskScheduleSummary {
    param(
        [Parameter(Mandatory)]
        [string]$TaskName
    )

    $task = Get-ScheduledTask -TaskName $TaskName
    $info = Get-ScheduledTaskInfo -TaskName $TaskName
    $xml = [xml](Export-ScheduledTask -TaskName $TaskName)
    $namespace = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
    $namespace.AddNamespace("t", "http://schemas.microsoft.com/windows/2004/02/mit/task")
    $calendar = $xml.SelectSingleNode("//t:CalendarTrigger", $namespace)
    $dayNodes = $xml.SelectNodes(
        "//t:CalendarTrigger/t:ScheduleByWeek/t:DaysOfWeek/*",
        $namespace
    )

    [pscustomobject]@{
        TaskName = $TaskName
        State = [string]$task.State
        Execute = [string]$task.Actions[0].Execute
        WorkingDirectory = [string]$task.Actions[0].WorkingDirectory
        StartBoundary = [string]$calendar.StartBoundary
        DaysOfWeek = @($dayNodes | ForEach-Object { $_.LocalName })
        NextRunTime = $info.NextRunTime.ToString("s")
        LastTaskResult = $info.LastTaskResult
        MultipleInstances = [string]$task.Settings.MultipleInstances
    }
}

$dailyTask = Get-ScheduledTask -TaskName $dailyTaskName
if ($dailyTask.Actions.Count -ne 1 -or $dailyTask.Actions[0].Execute -ne $dailyBatch) {
    throw "Unexpected action on '$dailyTaskName'; refusing to modify it."
}

$monthlyTask = Get-ScheduledTask -TaskName $monthlyTaskName
$existingDividendTask = Get-ScheduledTask -TaskName $dividendTaskName -ErrorAction SilentlyContinue
if ($null -ne $existingDividendTask) {
    if ($existingDividendTask.Actions.Count -ne 1 -or $existingDividendTask.Actions[0].Execute -ne $dividendBatch) {
        throw "Unexpected action on '$dividendTaskName'; refusing to overwrite it."
    }
}

New-Item -ItemType Directory -Path $BackupDirectory -Force | Out-Null
$dailyXmlBefore = Export-TaskBackup -TaskName $dailyTaskName -FileName "$dailyTaskName.xml"
$null = Export-TaskBackup -TaskName $monthlyTaskName -FileName "$monthlyTaskName.xml"
$dividendXmlBefore = $null
if ($null -ne $existingDividendTask) {
    $dividendXmlBefore = Export-TaskBackup -TaskName $dividendTaskName -FileName "$dividendTaskName.xml"
}

$dailyTrigger = New-ScheduledTaskTrigger `
    -Weekly `
    -WeeksInterval 1 `
    -DaysOfWeek $weekdays `
    -At "17:00"
$dividendTrigger = New-ScheduledTaskTrigger `
    -Weekly `
    -WeeksInterval 1 `
    -DaysOfWeek $weekdays `
    -At "18:00"
$dividendAction = New-ScheduledTaskAction `
    -Execute $dividendBatch `
    -WorkingDirectory $workingDirectory

try {
    Set-ScheduledTask -TaskName $dailyTaskName -Trigger $dailyTrigger | Out-Null
    Register-ScheduledTask `
        -TaskName $dividendTaskName `
        -Action $dividendAction `
        -Trigger $dividendTrigger `
        -Settings $dailyTask.Settings `
        -Principal $dailyTask.Principal `
        -Description "Runs the dividend workflow on weekdays after the daily workflow." `
        -Force | Out-Null

    $dailySummary = Get-TaskScheduleSummary -TaskName $dailyTaskName
    $dividendSummary = Get-TaskScheduleSummary -TaskName $dividendTaskName
    $expectedDays = ($weekdays | Sort-Object) -join ","

    if ((($dailySummary.DaysOfWeek | Sort-Object) -join ",") -ne $expectedDays) {
        throw "Daily task weekday verification failed."
    }
    if ((($dividendSummary.DaysOfWeek | Sort-Object) -join ",") -ne $expectedDays) {
        throw "Dividend task weekday verification failed."
    }
    if (([datetime]$dailySummary.StartBoundary).TimeOfDay -ne [timespan]::FromHours(17)) {
        throw "Daily task time verification failed."
    }
    if (([datetime]$dividendSummary.StartBoundary).TimeOfDay -ne [timespan]::FromHours(18)) {
        throw "Dividend task time verification failed."
    }
    if ($dailySummary.Execute -ne $dailyBatch -or $dividendSummary.Execute -ne $dividendBatch) {
        throw "Task action verification failed."
    }

    $result = [pscustomobject]@{
        BackupDirectory = $BackupDirectory
        WorkflowsExecuted = $false
        Daily = $dailySummary
        Dividend = $dividendSummary
        Monthly = [pscustomobject]@{
            TaskName = $monthlyTaskName
            State = [string]$monthlyTask.State
            Changed = $false
        }
    } | ConvertTo-Json -Depth 5
    if (-not [string]::IsNullOrWhiteSpace($ResultPath)) {
        Set-Content -LiteralPath $ResultPath -Value $result -Encoding UTF8
    }
    $result
}
catch {
    $failureMessage = $_.Exception.Message
    $rollbackMessage = $null
    try {
        Register-ScheduledTask -TaskName $dailyTaskName -Xml $dailyXmlBefore -Force | Out-Null
        if ($null -ne $dividendXmlBefore) {
            Register-ScheduledTask -TaskName $dividendTaskName -Xml $dividendXmlBefore -Force | Out-Null
        }
        else {
            $createdDividendTask = Get-ScheduledTask -TaskName $dividendTaskName -ErrorAction SilentlyContinue
            if ($null -ne $createdDividendTask) {
                Unregister-ScheduledTask -TaskName $dividendTaskName -Confirm:$false
            }
        }
    }
    catch {
        $rollbackMessage = $_.Exception.Message
    }

    $failureResult = [pscustomobject]@{
        Succeeded = $false
        Error = $failureMessage
        RollbackError = $rollbackMessage
        WorkflowsExecuted = $false
    } | ConvertTo-Json -Depth 3
    if (-not [string]::IsNullOrWhiteSpace($ResultPath)) {
        Set-Content -LiteralPath $ResultPath -Value $failureResult -Encoding UTF8
    }
    if ($null -ne $rollbackMessage) {
        throw "Task configuration failed: $failureMessage Rollback also failed: $rollbackMessage"
    }
    throw "Task configuration failed and was rolled back: $failureMessage"
}
