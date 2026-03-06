# 🚀 QUICK START GUIDE

Get up and running in 5 minutes!

## ⚡ Super Quick Setup (Copy & Paste)

### 1. Install Python & VS Code
- Download Python 3.9+: https://www.python.org/downloads/
- Download VS Code: https://code.visualstudio.com/

### 2. Open Project in VS Code
```bash
cd forex_prediction_bot
code .
```

### 3. Create Virtual Environment
**In VS Code Terminal** (View → Terminal or Ctrl+`):

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Mac/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 4. Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 5. Run Your First Analysis
```bash
python main_bot.py --full-analysis
```

That's it! 🎉

---

## 📊 Common Commands

```bash
# Full analysis (daily data)
python main_bot.py --full-analysis

# Hourly analysis
python main_bot.py --full-analysis --interval 1h

# Watch EUR/USD in real-time
python main_bot.py --watch "EUR/USD" --type forex --interval 15m

# Watch a stock
python main_bot.py --watch "AAPL" --type stock

# Use XGBoost model
python main_bot.py --full-analysis --model xgboost

# Quick check (no training)
python main_bot.py --full-analysis --no-train
```

---

## 🔧 If Something Goes Wrong

### "python not found"
Use `python3` instead of `python`:
```bash
python3 -m venv venv
python3 main_bot.py --full-analysis
```

### "Module not found"
Activate virtual environment first:
```bash
# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate

# Then install
pip install -r requirements.txt
```

### API Errors
The bot works with Yahoo Finance (no API key needed). For more features, add API keys to `config/config.py`:
- Alpha Vantage: https://www.alphavantage.co/support/#api-key
- Finnhub: https://finnhub.io/register

---

## 🎯 What Each Mode Does

### Full Analysis Mode
Analyzes ALL assets:
- Fetches data for forex, stocks, commodities, indices
- Calculates 50+ technical indicators
- Trains ML models
- Makes predictions
- Finds correlations
- Generates alerts
- Creates detailed reports

### Watch Mode
Monitors ONE asset in real-time:
- Updates every minute
- Shows current price & indicators
- Displays alerts
- Makes predictions
- Good for live trading

---

## 📁 Important Files

- **main_bot.py**: Run this file
- **config/config.py**: Configure assets & API keys
- **requirements.txt**: Dependencies
- **README.md**: Full documentation

---

## 🎮 VS Code Shortcuts

- **Run Task**: `Ctrl+Shift+P` → "Run Task" → Select task
- **Debug**: Press `F5` → Choose configuration
- **Terminal**: `` Ctrl+` ``
- **New Terminal**: Click `+` in terminal panel

---

## ⚠️ Remember

**This is for educational purposes only!**
- Not financial advice
- Trading carries risk
- You can lose money
- Do your own research

---

## 🎓 Next Steps

1. ✅ Get it running (you're here!)
2. 📖 Read the full [README.md](README.md)
3. ⚙️ Customize assets in `config/config.py`
4. 🧪 Experiment with different models and intervals
5. 📊 Analyze the results and learn

---

## 💡 Pro Tips

1. **Start with daily data** (faster, less API calls)
   ```bash
   python main_bot.py --full-analysis --interval 1d
   ```

2. **Use `--no-train` for quick checks** (after first run)
   ```bash
   python main_bot.py --full-analysis --no-train
   ```

3. **Focus on one asset** to understand behavior
   ```bash
   python main_bot.py --watch "EUR/USD" --type forex
   ```

4. **Check correlations** to understand market relationships

5. **Read the alerts** - they highlight important signals

---

## 🆘 Getting Help

1. Check the full [README.md](README.md)
2. Read error messages carefully
3. Google the error message
4. Check your virtual environment is activated
5. Make sure all dependencies are installed

---

**Ready to dive deeper? Check out the full README.md!**

Happy trading! 🚀📈
