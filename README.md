# 🤖 Forex & Multi-Asset Prediction Bot

A sophisticated machine learning-powered trading bot that analyzes and predicts price movements across multiple asset classes: Forex, Stocks, Commodities, Indices, and Cryptocurrencies.

## ⚠️ IMPORTANT DISCLAIMER

**THIS SOFTWARE IS FOR EDUCATIONAL AND INFORMATIONAL PURPOSES ONLY.**

- This bot does NOT provide financial advice
- Trading carries substantial risk of loss
- Past performance is NOT indicative of future results
- You could lose all of your invested capital
- Always do your own research (DYOR)
- Consider consulting with a licensed financial advisor
- The developers assume NO responsibility for trading losses

**Use at your own risk. You are solely responsible for your trading decisions.**

---

## 🌟 Features

### Multi-Asset Support
- **Forex**: EUR/USD, GBP/USD, USD/JPY, and more
- **Stocks**: AAPL, MSFT, GOOGL, TSLA, etc.
- **Commodities**: Gold, Silver, Crude Oil, Natural Gas
- **Indices**: S&P 500, NASDAQ, Dow Jones, FTSE
- **Cryptocurrencies**: BTC, ETH (via Yahoo Finance)

### Technical Analysis
- 50+ Technical Indicators
  - Moving Averages (SMA, EMA)
  - Oscillators (RSI, Stochastic, CCI)
  - Trend Indicators (MACD, ADX, Ichimoku)
  - Volatility (Bollinger Bands, ATR)
  - Volume Indicators (OBV)
  - Support/Resistance Detection
  - Candlestick Pattern Recognition

### Machine Learning Models
- **Random Forest**: Fast and reliable ensemble method
- **XGBoost**: Gradient boosting for better accuracy
- **LSTM**: Deep learning for time series (requires TensorFlow)
- **Ensemble**: Combines multiple models for robust predictions

### Advanced Analytics
- Correlation Analysis across assets
- Risk Metrics (Sharpe Ratio, Volatility, Max Drawdown)
- Real-time Alert System
- Automated Report Generation
- Feature Engineering (100+ derived features)

### Modes of Operation
1. **Full Analysis Mode**: Analyze all configured assets
2. **Watch Mode**: Monitor a single asset in real-time
3. **Custom Analysis**: Flexible API for custom workflows

---

## 📋 Prerequisites

- **Python 3.8+** (3.9-3.11 recommended)
- **VS Code** (Visual Studio Code)
- **Git** (optional, for version control)
- Internet connection for data fetching

---

## 🚀 VS Code Setup Guide

### Step 1: Install VS Code

1. Download VS Code from: https://code.visualstudio.com/
2. Install for your operating system (Windows/Mac/Linux)
3. Launch VS Code

### Step 2: Install Python Extension

1. Open VS Code
2. Click the Extensions icon (or press `Ctrl+Shift+X` / `Cmd+Shift+X`)
3. Search for "Python"
4. Install the official "Python" extension by Microsoft
5. Restart VS Code if prompted

### Step 3: Open the Project

**Option A: Using VS Code Interface**
1. Click `File` → `Open Folder`
2. Navigate to the `forex_prediction_bot` folder
3. Click "Select Folder"

**Option B: Using Command Line**
```bash
cd /path/to/forex_prediction_bot
code .
```

### Step 4: Set Up Python Environment

**Option A: Using Virtual Environment (Recommended)**

1. Open VS Code Terminal (`View` → `Terminal` or `` Ctrl+` ``)

2. Create virtual environment:
```bash
# Windows
python -m venv venv

# macOS/Linux
python3 -m venv venv
```

3. Activate virtual environment:
```bash
# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

4. Your terminal prompt should now show `(venv)` prefix

**Option B: Using Conda**
```bash
conda create -n forex_bot python=3.10
conda activate forex_bot
```

### Step 5: Install Dependencies

With your virtual environment activated:

```bash
# Upgrade pip first
pip install --upgrade pip

# Install all dependencies
pip install -r requirements.txt

# Optional: Install deep learning support (for LSTM)
pip install tensorflow

# Optional: Install visualization tools
pip install matplotlib seaborn plotly
```

**Verify installation:**
```bash
python -c "import pandas, numpy, sklearn, yfinance; print('✓ All core packages installed')"
```

### Step 6: Select Python Interpreter in VS Code

1. Press `Ctrl+Shift+P` (Windows/Linux) or `Cmd+Shift+P` (Mac)
2. Type "Python: Select Interpreter"
3. Choose the interpreter from your virtual environment:
   - Should show path like `./venv/bin/python` or `.\venv\Scripts\python.exe`

### Step 7: Configure API Keys

1. Open `config/config.py` in VS Code
2. Replace placeholder API keys with your actual keys:

```python
ALPHA_VANTAGE_API_KEY = "your_actual_key_here"
FINNHUB_API_KEY = "your_actual_key_here"
TWELVE_DATA_API_KEY = "your_actual_key_here"
```

**Get Free API Keys:**
- Alpha Vantage: https://www.alphavantage.co/support/#api-key
- Finnhub: https://finnhub.io/register
- Twelve Data: https://twelvedata.com/pricing

**Note:** The bot works with Yahoo Finance by default (no API key needed), but other sources provide more data and higher rate limits.

### Step 8: Customize Assets

Edit `config/config.py` to add/remove assets you want to track:

```python
FOREX_PAIRS = ["EUR/USD", "GBP/USD", "USD/JPY"]  # Add your pairs
STOCKS = ["AAPL", "MSFT", "GOOGL"]  # Add your stocks
COMMODITIES = ["GC=F", "CL=F"]  # Gold, Crude Oil
```

---

## 🎮 Running the Bot

### Method 1: Using VS Code Terminal

1. Open Terminal in VS Code: `View` → `Terminal`
2. Ensure virtual environment is activated (you should see `(venv)`)
3. Run the bot:

```bash
# Full analysis of all assets
python main_bot.py --full-analysis

# Use different time intervals
python main_bot.py --full-analysis --interval 1h
python main_bot.py --full-analysis --interval 15m

# Skip model training (faster, uses cached models)
python main_bot.py --full-analysis --no-train

# Watch a specific asset in real-time
python main_bot.py --watch "EUR/USD" --type forex --interval 15m

# Watch a stock
python main_bot.py --watch "AAPL" --type stock --interval 1h

# Use different ML models
python main_bot.py --full-analysis --model xgboost
python main_bot.py --full-analysis --model rf
```

### Method 2: Using VS Code Run Configuration

1. Open `main_bot.py` in VS Code
2. Press `F5` or click `Run` → `Start Debugging`
3. Select "Python File"

### Method 3: Using VS Code Python Extension

1. Open `main_bot.py`
2. Click the ▶️ play button in the top-right corner

---

## 📊 Understanding the Output

### Full Analysis Output

```
FOREX & MULTI-ASSET PREDICTION BOT
==================================================
⚠️  DISCLAIMER: For educational purposes only.

FETCHING MARKET DATA (Interval: 1d)
==================================================
  forex: EUR/USD, GBP/USD...
  stocks: AAPL, MSFT...
✓ Successfully fetched data for 25 assets

CALCULATING TECHNICAL INDICATORS
==================================================
  Processing forex_EUR/USD...
  Processing stocks_AAPL...
✓ Added indicators to 25 assets

TRAINING PREDICTION MODELS (ENSEMBLE)
==================================================
  Training model for forex_EUR/USD...
  Model Performance:
    MSE: 0.000123
    MAE: 0.008945
    R²: 0.8532
✓ Trained 25 models

GENERATING PREDICTIONS
==================================================
  forex_EUR/USD: 📈 UP (78% confidence, +0.45%)
  stocks_AAPL: 📉 DOWN (65% confidence, -1.23%)
  ...

CORRELATION ANALYSIS
==================================================
Found 5 highly correlated pairs:
  forex_EUR/USD ↔ forex_GBP/USD: 0.856
  ...

ALERT MONITORING
==================================================
  forex_EUR/USD:
    🚨 [MACD_BULLISH_CROSS] MACD bullish crossover
      → Consider buying
  stocks_AAPL:
    ⚠️ [RSI_OVERBOUGHT] RSI is overbought at 72.45
      → Consider selling
```

---

## 🛠️ VS Code Tips & Tricks

### Useful VS Code Shortcuts

- **Run Python File**: `Ctrl+Shift+P` → "Run Python File in Terminal"
- **Toggle Terminal**: `` Ctrl+` ``
- **Clear Terminal**: Type `clear` (Mac/Linux) or `cls` (Windows)
- **Split Terminal**: Click the split icon in terminal
- **Multiple Terminals**: Click the `+` icon in terminal

### Debugging in VS Code

1. Set breakpoints by clicking left of line numbers
2. Press `F5` to start debugging
3. Use debug controls:
   - `F10`: Step over
   - `F11`: Step into
   - `F5`: Continue
   - `Shift+F5`: Stop

### Recommended VS Code Extensions

1. **Python** (Microsoft) - Already installed
2. **Pylance** - Advanced Python language support
3. **Python Indent** - Correct Python indentation
4. **autoDocstring** - Generate docstrings
5. **GitLens** - Git integration (if using Git)
6. **Better Comments** - Colorful comments
7. **Error Lens** - Inline error display

Install via: `Ctrl+Shift+X` → Search → Install

### Create VS Code Tasks

Create `.vscode/tasks.json` for quick commands:

```json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "Run Full Analysis",
            "type": "shell",
            "command": "python main_bot.py --full-analysis",
            "group": {
                "kind": "build",
                "isDefault": true
            }
        },
        {
            "label": "Watch EUR/USD",
            "type": "shell",
            "command": "python main_bot.py --watch 'EUR/USD' --type forex"
        }
    ]
}
```

Run tasks: `Ctrl+Shift+P` → "Run Task"

---

## 📁 Project Structure

```
forex_prediction_bot/
│
├── config/
│   ├── __init__.py
│   └── config.py              # Configuration & API keys
│
├── data/
│   ├── __init__.py
│   └── fetcher.py             # Multi-source data fetching
│
├── indicators/
│   ├── __init__.py
│   └── technical.py           # Technical indicators
│
├── models/
│   ├── __init__.py
│   └── predictor.py           # ML prediction engine
│
├── utils/
│   ├── __init__.py
│   └── analysis.py            # Analytics & alerts
│
├── main_bot.py                # Main orchestrator
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

---

## 🔧 Troubleshooting

### "Python not found"
- Make sure Python is installed: `python --version`
- Add Python to PATH (Windows)
- Use `python3` instead of `python` on Mac/Linux

### "Module not found" errors
```bash
# Activate virtual environment first!
pip install -r requirements.txt
```

### "No module named 'config'"
```bash
# Make sure you're in the project root directory
cd forex_prediction_bot
python main_bot.py
```

### API Rate Limits
- Free API keys have rate limits
- Yahoo Finance has no API key but is rate-limited
- Wait between requests or upgrade API plans
- Bot includes automatic rate limiting (0.5s delays)

### TensorFlow/LSTM Issues
```bash
# LSTM is optional, use other models if TensorFlow fails
python main_bot.py --full-analysis --model ensemble
python main_bot.py --full-analysis --model xgboost
```

### Insufficient Data
- Some assets may not have enough historical data
- Bot automatically skips assets with <50 data points
- Try different time intervals

---

## 🎯 Usage Examples

### Example 1: Quick Daily Analysis
```bash
# Analyze all assets with daily data (fastest)
python main_bot.py --full-analysis --interval 1d
```

### Example 2: Intraday Trading
```bash
# Hourly analysis for day trading
python main_bot.py --full-analysis --interval 1h --model xgboost
```

### Example 3: Forex Scalping
```bash
# Watch EUR/USD every 15 minutes for 2 hours
python main_bot.py --watch "EUR/USD" --type forex --interval 15m --duration 120
```

### Example 4: Stock Analysis
```bash
# Watch AAPL in real-time
python main_bot.py --watch "AAPL" --type stock --interval 1h
```

### Example 5: Fast Check (No Training)
```bash
# Quick analysis using existing models
python main_bot.py --full-analysis --no-train
```

---

## 📚 Advanced Usage

### Using as a Python Library

```python
from main_bot import ForexPredictionBot

# Create bot
bot = ForexPredictionBot(model_type="ensemble")

# Fetch data
data = bot.fetch_all_market_data(interval="1d")

# Add indicators
data = bot.add_technical_indicators(data)

# Train models
bot.train_models(data)

# Make predictions
predictions = bot.generate_predictions(data)

# Analyze specific asset
for name, prediction in predictions.items():
    if prediction['confidence'] > 0.7:
        print(f"{name}: {prediction['direction']} ({prediction['confidence']:.1%})")
```

### Custom Asset Analysis

```python
from data.fetcher import DataFetcher
from indicators.technical import TechnicalIndicators
from models.predictor import PredictionEngine

# Fetch data for custom asset
fetcher = DataFetcher()
df = fetcher.fetch_stock_data("NVDA", interval="1d", lookback=200)

# Add indicators
df = TechnicalIndicators.add_all_indicators(df)

# Train & predict
engine = PredictionEngine(model_type="xgboost")
engine.train(df, target_periods=5)
prediction = engine.predict_next(df)

print(f"Prediction: {prediction['direction']} ({prediction['confidence']:.1%})")
```

---

## 🔐 Security Best Practices

1. **Never commit API keys to Git**
   ```bash
   # Add to .gitignore
   echo "config/config.py" >> .gitignore
   ```

2. **Use environment variables** (optional but recommended)
   ```python
   import os
   API_KEY = os.getenv('ALPHA_VANTAGE_KEY', 'default_key')
   ```

3. **Keep dependencies updated**
   ```bash
   pip list --outdated
   pip install --upgrade package_name
   ```

---

## 🤝 Contributing

This is an educational project. Feel free to:
- Report bugs via GitHub Issues
- Suggest improvements
- Fork and modify for your needs
- Share your learnings

---

## 📄 License

This project is provided "as-is" for educational purposes.

---

## 🙏 Acknowledgments

- **Data Sources**: Yahoo Finance, Alpha Vantage, Finnhub
- **ML Libraries**: scikit-learn, XGBoost, TensorFlow
- **Analysis**: pandas, numpy

---

## 📞 Support

- **Issues**: Open an issue on GitHub
- **Documentation**: See code comments and docstrings
- **Learning Resources**:
  - [Python for Finance](https://www.python.org/)
  - [Technical Analysis Basics](https://www.investopedia.com/)
  - [Machine Learning Trading](https://scikit-learn.org/)

---

## ⚡ Quick Start Checklist

- [ ] Install Python 3.8+
- [ ] Install VS Code
- [ ] Install Python extension in VS Code
- [ ] Clone/download project
- [ ] Open project folder in VS Code
- [ ] Create virtual environment: `python -m venv venv`
- [ ] Activate virtual environment
- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Configure API keys in `config/config.py` (optional)
- [ ] Run first analysis: `python main_bot.py --full-analysis`
- [ ] Profit! (kidding - remember the disclaimer 😊)

---

**Remember: This is a learning tool, not a get-rich-quick scheme. Trade responsibly!**

Good luck, and happy trading! 📈🤖
