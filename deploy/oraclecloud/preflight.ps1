$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location ..\..

function Get-EnvMap {
    $envPath = Join-Path (Get-Location) ".env"
    if (-not (Test-Path $envPath)) {
        throw ".env not found"
    }

    $map = @{}
    foreach ($line in Get-Content $envPath) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#") -or -not $trimmed.Contains("=")) {
            continue
        }
        $key, $value = $trimmed.Split("=", 2)
        $map[$key] = $value.Trim()
    }
    return $map
}

function Get-EnvValue {
    param(
        [hashtable]$EnvMap,
        [string]$Key,
        [string]$Default = ""
    )

    if ($EnvMap.ContainsKey($Key) -and $null -ne $EnvMap[$Key] -and $EnvMap[$Key] -ne "") {
        return $EnvMap[$Key]
    }
    return $Default
}

function Show-EnvChecks {
    param([hashtable]$EnvMap)

    Write-Host "== Environment checks =="
    $developmentMode = Get-EnvValue -EnvMap $EnvMap -Key "DEVELOPMENT_MODE" -Default "<unset>"
    $dashboardCors = Get-EnvValue -EnvMap $EnvMap -Key "DASHBOARD_CORS_ORIGINS" -Default "<unset>"
    $dashboardApiKey = Get-EnvValue -EnvMap $EnvMap -Key "DASHBOARD_API_KEY"

    "{0,-28}{1}" -f "DEVELOPMENT_MODE", $developmentMode
    "{0,-28}{1}" -f "DASHBOARD_CORS_ORIGINS", $dashboardCors
    "{0,-28}{1}" -f "DASHBOARD_API_KEY", ($(if ($dashboardApiKey) { "<set>" } else { "<unset>" }))

    if ($developmentMode.ToLowerInvariant() -ne "false") {
        Write-Warning "DEVELOPMENT_MODE should be false in production"
    }
    if ($dashboardCors -match "localhost|127\.0\.0\.1") {
        Write-Warning "DASHBOARD_CORS_ORIGINS still points at localhost"
    }
    if (-not $dashboardApiKey -or $dashboardApiKey -like "replace-*") {
        Write-Warning "DASHBOARD_API_KEY is missing or still a placeholder"
    }
    $databaseUrl = Get-EnvValue -EnvMap $EnvMap -Key "DATABASE_URL"
    if (-not $databaseUrl -or $databaseUrl -match "replace-password") {
        Write-Warning "DATABASE_URL is missing or still a placeholder"
    }
}

function Show-CoreRuntimeKeys {
    param([hashtable]$EnvMap)

    Write-Host ""
    Write-Host "== Core runtime keys =="

    $required = @(
        "OPENAI_API_KEY",
        "DERIV_APP_ID",
        "DERIV_TOKEN",
        "COMMAND_BOT_TOKEN",
        "COMMAND_BOT_CHAT_ID",
        "WHALE_TELEGRAM_TOKEN",
        "INTELLIGENCE_CHAT_ID",
        "TELEGRAM_API_ID",
        "TELEGRAM_API_HASH",
        "TELEGRAM_PHONE"
    )

    $missing = @()
    foreach ($key in $required) {
        $value = Get-EnvValue -EnvMap $EnvMap -Key $key
        $isSet = ($value -and ($value -notlike "replace-*"))
        "{0,-28}{1}" -f $key, ($(if ($isSet) { "<set>" } else { "<missing>" }))
        if (-not $isSet) {
            $missing += $key
        }
    }

    if ($missing.Count -gt 0) {
        Write-Warning ("Missing core runtime keys: " + ($missing -join ", "))
    }
}

function Show-LocalServiceChecks {
    Write-Host ""
    Write-Host "== Local service checks =="
    Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in 5000, 8081, 9100 } |
        Select-Object LocalAddress, LocalPort, OwningProcess, State
}

function Show-HttpChecks {
    param([hashtable]$EnvMap)

    Write-Host ""
    Write-Host "== HTTP status check =="

    if (-not $EnvMap["DASHBOARD_API_KEY"]) {
        Write-Host "DASHBOARD_API_KEY not found in .env"
        return
    }

    try {
        $login = Invoke-RestMethod -Uri "http://127.0.0.1:5000/api/login" `
            -Method POST `
            -ContentType "application/json" `
            -Body (@{ api_key = $EnvMap["DASHBOARD_API_KEY"] } | ConvertTo-Json) `
            -TimeoutSec 10
        if (-not $login.token) {
            Write-Host "FAILED to obtain dashboard token"
            return
        }

        $status = Invoke-RestMethod -Uri "http://127.0.0.1:5000/api/status" `
            -Headers @{ Authorization = "Bearer $($login.token)" } `
            -TimeoutSec 10
        $status | ConvertTo-Json -Depth 6
    } catch {
        Write-Warning "HTTP status check failed: $($_.Exception.Message)"
    }
}

function Show-ReverseProxyCheck {
    Write-Host ""
    Write-Host "== Reverse proxy check =="
    $listeners = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in 80, 443 }
    if (-not $listeners) {
        Write-Host "No local listener on 80/443"
        return
    }
    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1/" -Method Head -TimeoutSec 10 -UseBasicParsing
        "{0} {1}" -f [int]$resp.StatusCode, $resp.StatusDescription
    } catch {
        Write-Warning "Reverse proxy probe failed: $($_.Exception.Message)"
    }
}

function Show-OutboundChecks {
    Write-Host ""
    Write-Host "== Outbound connectivity checks =="
    $targets = @(
        @{ Host = "api.telegram.org"; Port = 443 },
        @{ Host = "api.derivws.com"; Port = 443 },
        @{ Host = "stream.binance.com"; Port = 9443 },
        @{ Host = "stream.bybit.com"; Port = 443 },
        @{ Host = "www.reddit.com"; Port = 443 },
        @{ Host = "bsc-dataseed1.binance.org"; Port = 443 },
        @{ Host = "api.mainnet-beta.solana.com"; Port = 443 },
        @{ Host = "s1.ripple.com"; Port = 51234 }
    )

    foreach ($target in $targets) {
        $ok = $false
        try {
            $ok = Test-NetConnection -ComputerName $target.Host -Port $target.Port -InformationLevel Quiet -WarningAction SilentlyContinue
        } catch {
            $ok = $false
        }
        "{0,-35}{1}" -f "$($target.Host):$($target.Port)", ($(if ($ok) { "OK" } else { "FAIL" }))
    }
}

function Show-LogTail {
    Write-Host ""
    Write-Host "== Recent warning/error tail =="
    $logPath = Join-Path (Get-Location) "logs\\trading_bot.log"
    if (Test-Path $logPath) {
        Get-Content $logPath -Tail 80
    } else {
        Write-Host "logs\\trading_bot.log not found"
    }
}

$envMap = Get-EnvMap
Show-EnvChecks -EnvMap $envMap
Show-CoreRuntimeKeys -EnvMap $envMap
Show-LocalServiceChecks
Show-HttpChecks -EnvMap $envMap
Show-ReverseProxyCheck
Show-OutboundChecks
Show-LogTail
