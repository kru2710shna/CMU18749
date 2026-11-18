<#
.SYNOPSIS
Runs the complete Milestone 5 system locally with one Windows console per component.

.DESCRIPTION
RM, GFD, every LFD, every initial replica, and every client receive independent
console windows. An RM-recovered replica also opens a replacement console, so
its server log never gets mixed into the LFD console.

.EXAMPLE
.\run_m5_windows.ps1 -Mode active

.EXAMPLE
.\run_m5_windows.ps1 -Mode passive -PrimaryReplica 3
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("active", "passive")]
    [string]$Mode,

    [ValidateSet(1, 2, 3)]
    [int]$PrimaryReplica = 1,

    [double]$HeartbeatFreq = 1,
    [double]$CheckpointFreq = 2,
    [double]$RequestInterval = 1,
    [double]$StateTransferDelay = 3,
    [double]$RecoveryRetry = 2,
    [int]$RmPort = 6100,
    [int]$GfdPort = 6000,
    [int]$ReplicaBasePort = 5000,
    [double]$StartupWait = 6
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonBin = if ($env:PYTHON_BIN) { $env:PYTHON_BIN } else { "python" }
$ShellExe = (Get-Process -Id $PID).Path
$Components = [System.Collections.Generic.List[object]]::new()

if (-not (Get-Command $PythonBin -ErrorAction SilentlyContinue)) {
    throw "Python executable '$PythonBin' was not found. Set PYTHON_BIN if needed."
}

Get-ChildItem -LiteralPath (Join-Path $ProjectDir "src") -Filter "*.py" |
    ForEach-Object { & $PythonBin -m py_compile $_.FullName }
& $PythonBin -m py_compile (Join-Path $ProjectDir "launch_component_terminal.py")

function ConvertTo-PsLiteral {
    param([Parameter(Mandatory = $true)][string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Start-ComponentConsole {
    param(
        [Parameter(Mandatory = $true)][string]$Title,
        [Parameter(Mandatory = $true)][string[]]$PythonArguments
    )

    $argv = @($PythonBin) + $PythonArguments
    $invocation = "& " + (($argv | ForEach-Object {
        ConvertTo-PsLiteral ([string]$_)
    }) -join " ")
    $body = '$Host.UI.RawUI.WindowTitle = ' + (ConvertTo-PsLiteral $Title) +
        '; Set-Location -LiteralPath ' + (ConvertTo-PsLiteral $ProjectDir) +
        '; ' + $invocation
    $encoded = [Convert]::ToBase64String(
        [Text.Encoding]::Unicode.GetBytes($body)
    )
    $process = Start-Process -FilePath $ShellExe -ArgumentList @(
        "-NoProfile",
        "-NoExit",
        "-EncodedCommand",
        $encoded
    ) -PassThru
    $Components.Add([pscustomobject]@{
        Title = $Title
        Process = $process
    })
    return $process
}

function Get-ServerArguments {
    param([Parameter(Mandatory = $true)][int]$ReplicaNumber)

    $replicaId = "S$ReplicaNumber"
    $replicaPort = $ReplicaBasePort + $ReplicaNumber
    $arguments = @(
        "-u", (Join-Path $ProjectDir "src\server_replica.py"),
        "--replica-id", $replicaId,
        "--host", "127.0.0.1",
        "--port", [string]$replicaPort,
        "--replication-mode", $Mode,
        "--rm-host", "127.0.0.1",
        "--rm-port", [string]$RmPort,
        "--state-transfer-delay", [string]$StateTransferDelay
    )
    if ($Mode -eq "passive") {
        $arguments += @("--checkpoint-freq", [string]$CheckpointFreq)
    }
    return [string[]]$arguments
}

try {
    # Fault-tolerance infrastructure.
    $null = Start-ComponentConsole "RM" @(
        "-u", (Join-Path $ProjectDir "src\replication_manager.py"),
        "--host", "127.0.0.1",
        "--port", [string]$RmPort,
        "--replication-mode", $Mode,
        "--auto-recovery",
        "--recovery-retry", [string]$RecoveryRetry
    )
    Start-Sleep -Milliseconds 500
    $null = Start-ComponentConsole "GFD" @(
        "-u", (Join-Path $ProjectDir "src\global_fault_detector.py"),
        "--host", "127.0.0.1",
        "--port", [string]$GfdPort,
        "--rm-host", "127.0.0.1",
        "--rm-port", [string]$RmPort
    )

    # LFDs. The recovery command invokes a small waiting helper that creates a
    # new server console. Because the helper remains alive, duplicate RM retry
    # messages cannot create duplicate replica processes.
    foreach ($replicaNumber in 1..3) {
        $replicaId = "S$replicaNumber"
        $lfdId = "LFD$replicaNumber"
        $replicaPort = $ReplicaBasePort + $replicaNumber
        $serverArguments = Get-ServerArguments $replicaNumber
        $serverCommand = @($PythonBin) + $serverArguments
        $recoveryCommand = @(
            $PythonBin,
            "-u",
            (Join-Path $ProjectDir "launch_component_terminal.py"),
            "--backend", "windows",
            "--title", "$replicaId-recovered"
        ) + $serverCommand
        $lfdArguments = @(
            "-u", (Join-Path $ProjectDir "src\local_fault_detector.py"),
            "--lfd-id", $lfdId,
            "--replica-id", $replicaId,
            "--server-host", "127.0.0.1",
            "--server-port", [string]$replicaPort,
            "--heartbeat-freq", [string]$HeartbeatFreq,
            "--gfd-host", "127.0.0.1",
            "--gfd-port", [string]$GfdPort,
            "--replica-public-host", "127.0.0.1",
            "--recovery-command"
        ) + $recoveryCommand
        $null = Start-ComponentConsole $lfdId ([string[]]$lfdArguments)
    }

    # Start the selected passive primary before the other replicas. The same
    # ordering option is harmless and deterministic in active mode.
    $null = Start-ComponentConsole "S$PrimaryReplica" (
        Get-ServerArguments $PrimaryReplica
    )
    Start-Sleep -Seconds 2
    foreach ($replicaNumber in 1..3) {
        if ($replicaNumber -eq $PrimaryReplica) {
            continue
        }
        $null = Start-ComponentConsole "S$replicaNumber" (
            Get-ServerArguments $replicaNumber
        )
    }

    Start-Sleep -Seconds $StartupWait
    foreach ($clientNumber in 1..3) {
        $null = Start-ComponentConsole "C$clientNumber" @(
            "-u", (Join-Path $ProjectDir "src\client.py"),
            "--client-id", "C$clientNumber",
            "--gfd-host", "127.0.0.1",
            "--gfd-port", [string]$GfdPort,
            "--replication-mode", $Mode,
            "--add-amount", [string]$clientNumber,
            "--request-interval", [string]$RequestInterval
        )
    }

    Write-Host ""
    Write-Host "Milestone 5 local $Mode demo is running in separate consoles."
    Write-Host "Initial passive primary: S$PrimaryReplica"
    Write-Host ""
    Write-Host "Fault injection:"
    Write-Host "  Press Ctrl-C in an SN console. The RM opens a new console named"
    Write-Host "  SN-recovered. Press Ctrl-C there to fault the recovered process."
    Write-Host ""
    Read-Host "Press Enter here to stop every component"
}
finally {
    # Each tracked PowerShell is the root of one component tree. /T also stops
    # a recovered server launched beneath an LFD's terminal helper.
    foreach ($component in $Components) {
        try {
            $component.Process.Refresh()
            if (-not $component.Process.HasExited) {
                & taskkill.exe /PID $component.Process.Id /T /F 2>$null | Out-Null
            }
        }
        catch {
            Write-Warning "Could not stop $($component.Title): $_"
        }
    }
}
