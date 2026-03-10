# 🏥 Setup Health Check Task
# Run this in PowerShell AS ADMINISTRATOR

$TaskName = "TradingBotHealthCheck"
$ScriptPath = "C:\Users\ROBBIE\Downloads\forex_prediction_bot\health_check.py"
$PythonPath = "C:\Users\ROBBIE\Downloads\forex_prediction_bot\venv311\Scripts\python.exe"
$WorkingDirectory = "C:\Users\ROBBIE\Downloads\forex_prediction_bot"

Write-Host "🏥 SETTING UP HEALTH CHECK TASK" -ForegroundColor Cyan
Write-Host ("="*70) -ForegroundColor Cyan

# Check if running as administrator
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
$isAdmin = $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host "❌ ERROR: This script must be run as Administrator!" -ForegroundColor Red
    Write-Host "Right-click PowerShell and select 'Run as Administrator'" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ Running as Administrator" -ForegroundColor Green

# Check if files exist
if (-not (Test-Path $ScriptPath)) {
    Write-Host "❌ ERROR: Health check script not found at: $ScriptPath" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $PythonPath)) {
    Write-Host "❌ ERROR: Python not found at: $PythonPath" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Found health check script" -ForegroundColor Green
Write-Host "✅ Found Python executable" -ForegroundColor Green

# Remove existing task if it exists
try {
    Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop | Out-Null
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "🗑️ Removed existing task" -ForegroundColor Yellow
} catch {
    # Task doesn't exist, that's fine
}

# Create the scheduled task action
$Action = New-ScheduledTaskAction `
    -Execute $PythonPath `
    -Argument "`"$ScriptPath`"" `
    -WorkingDirectory $WorkingDirectory

# Create the trigger (hourly) - FIXED SYNTAX
$Trigger = New-ScheduledTaskTrigger -Daily -At "00:00"
$Trigger.Repetition = New-ScheduledTaskTriggerRepetition `
    -Interval (New-TimeSpan -Hours 1) `
    -Duration (New-TimeSpan -Days 365)

# Create task settings
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -RestartInterval (New-TimeSpan -Minutes 10) `
    -RestartCount 3

# Create the task principal (run as SYSTEM)
$Principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

# Register the scheduled task
try {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $Action `
        -Trigger $Trigger `
        -Settings $Settings `
        -Principal $Principal `
        -Description "Hourly health check for Trading Bot" `
        -Force | Out-Null
    
    Write-Host ""
    Write-Host "✅ SUCCESS! Health check task created!" -ForegroundColor Green
    Write-Host ("="*70) -ForegroundColor Green
    Write-Host ""
    Write-Host "📅 Schedule: Every hour (starting at midnight)" -ForegroundColor Cyan
    Write-Host "📁 Script: $ScriptPath" -ForegroundColor Cyan
    Write-Host "🐍 Python: $PythonPath" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "💡 To manage the task:" -ForegroundColor Yellow
    Write-Host "   1. Open Task Scheduler (taskschd.msc)" -ForegroundColor White
    Write-Host "   2. Find '$TaskName' in Task Scheduler Library" -ForegroundColor White
    Write-Host ""
    Write-Host "🧪 To test immediately:" -ForegroundColor Yellow
    Write-Host "   Start-ScheduledTask -TaskName '$TaskName'" -ForegroundColor White
    Write-Host ""
    
} catch {
    Write-Host "❌ ERROR: Failed to create scheduled task" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

# Test the task
Write-Host "🧪 Testing if task was created successfully..." -ForegroundColor Cyan
try {
    $Task = Get-ScheduledTask -TaskName $TaskName
    Write-Host "✅ Task verified and ready!" -ForegroundColor Green
    
    # Ask if user wants to run now
    Write-Host ""
    $response = Read-Host "🚀 Would you like to run health check NOW to test? (Y/N)"
    if ($response -eq 'Y' -or $response -eq 'y') {
        Write-Host "⏳ Running health check..." -ForegroundColor Yellow
        Start-ScheduledTask -TaskName $TaskName
        Write-Host "✅ Health check started! Check logs in health_log.txt" -ForegroundColor Green
    }
    
} catch {
    Write-Host "⚠️ Warning: Could not verify task" -ForegroundColor Yellow
}

Write-Host ""
Write-Host ("="*70) -ForegroundColor Cyan
Write-Host "🎉 SETUP COMPLETE!" -ForegroundColor Green
Write-Host ("="*70) -ForegroundColor Cyan
Write-Host ""