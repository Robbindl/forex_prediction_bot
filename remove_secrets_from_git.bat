@echo off
REM ══════════════════════════════════════════════════════════════
REM  Remove secrets from git history permanently
REM  Run once from: C:\Users\ROBBIE\Downloads\forex_prediction_bot
REM ══════════════════════════════════════════════════════════════

echo Removing secrets from git history...

REM Remove secret files from ALL git history
git filter-branch --force --index-filter ^
  "git rm --cached --ignore-unmatch config/telegram_config.json config/email_config.json twitter_cookies.json" ^
  --prune-empty --tag-name-filter cat -- --all

REM Clean up
git for-each-ref --format="delete %(refname)" refs/original | git update-ref --stdin
git reflog expire --expire=now --all
git gc --prune=now --aggressive

REM Force push (overwrites remote history — required)
echo.
echo Force pushing clean history to GitHub...
git push origin --force --all
git push origin --force --tags

echo.
echo DONE. Secrets removed from git history.
echo The files still exist locally (they are gitignored now).
echo Go to GitHub Settings and regenerate your Telegram bot tokens.
pause
