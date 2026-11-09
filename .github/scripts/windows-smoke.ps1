# Runs a Windows nisshi.exe through the quickstart: it starts a broker on the
# default storage engine and listener, creates a topic, sends a message, reads
# it back and lists the topics. It fails on the first step that fails, and
# prints the broker's output either way.
#
# The broker starts without --listener-url, so it binds [::]:9092, and the
# commands connect to 127.0.0.1:9092. Windows sets IPV6_V6ONLY on a new IPv6
# socket, unlike Linux and macOS, so this checks that the broker accepts IPv4
# connections on its dual-stack listener.
#
# Usage: windows-smoke.ps1 -Exe <path to nisshi.exe>
param(
    [Parameter(Mandatory)]
    [string] $Exe
)

$ErrorActionPreference = 'Stop'

# A full path, because Start-Process and & resolve a relative path differently.
$Exe = (Resolve-Path $Exe).Path

# Runs nisshi.exe with the given arguments and returns its output. It throws
# if the exe exits with a status other than 0, because PowerShell does not
# stop on a native command's exit status.
function Invoke-Nisshi {
    $output = & $Exe @args 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "nisshi $args exited with status ${LASTEXITCODE}:`n$output"
    }
    Write-Host "> nisshi $args`n$output"
    return $output
}

$version = Invoke-Nisshi --version
if ($version -notmatch '^nisshi \d+\.\d+\.\d+') {
    throw "unexpected --version output: $version"
}

$logs = Join-Path $env:RUNNER_TEMP nisshi-smoke
New-Item -ItemType Directory -Path $logs -Force | Out-Null
$stdout = Join-Path $logs broker.out
$stderr = Join-Path $logs broker.err
$broker = Start-Process -FilePath $Exe -NoNewWindow -PassThru `
    -RedirectStandardOutput $stdout -RedirectStandardError $stderr

try {
    # The broker prints "ready in <n>ms" once its listener accepts connections.
    $deadline = (Get-Date).AddSeconds(60)
    while (-not (Select-String -Path $stdout -Pattern '^ready in ' -Quiet)) {
        if ($broker.HasExited) {
            $status = $broker.ExitCode
            throw "the broker exited with status $status before it was ready"
        }
        if ((Get-Date) -gt $deadline) {
            throw 'the broker was not ready after 60 seconds'
        }
        Start-Sleep -Milliseconds 500
    }

    Invoke-Nisshi topic create hello | Out-Null

    $message = '{"value": "hello world"}'
    $produced = $message | & $Exe cat produce hello 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "cat produce exited with status ${LASTEXITCODE}:`n$produced"
    }
    Write-Host "> nisshi cat produce hello`n$produced"

    $consumed = Invoke-Nisshi cat consume hello
    if ($consumed -notmatch '"value":"hello world"') {
        throw 'cat consume did not print the message that cat produce sent'
    }

    $topics = Invoke-Nisshi topic list
    if ($topics -notmatch '(?m)^hello\s+\d+\s*$') {
        throw 'topic list did not list the topic hello'
    }
} finally {
    Stop-Process -Id $broker.Id -Force -ErrorAction SilentlyContinue
    Write-Host '--- broker stdout'
    Get-Content $stdout -ErrorAction SilentlyContinue
    Write-Host '--- broker stderr'
    Get-Content $stderr -ErrorAction SilentlyContinue
}
