# 💻 VS Code Complete Setup Guide

This guide covers everything you need to know about using VS Code with this project.

## 📦 Initial Setup

### 1. Open Project in VS Code

**Method 1: From VS Code**
1. Open VS Code
2. File → Open Folder (Ctrl+K Ctrl+O)
3. Navigate to `forex_prediction_bot` folder
4. Click "Select Folder"

**Method 2: From Terminal**
```bash
cd forex_prediction_bot
code .
```

### 2. Install Python Extension

1. Click Extensions icon (Ctrl+Shift+X)
2. Search "Python"
3. Install "Python" by Microsoft
4. Install "Pylance" (should install automatically)
5. Restart VS Code

### 3. Select Python Interpreter

1. Open Command Palette (Ctrl+Shift+P)
2. Type "Python: Select Interpreter"
3. Choose your virtual environment:
   - `./venv/bin/python` (Mac/Linux)
   - `.\venv\Scripts\python.exe` (Windows)

If you don't see your venv:
- Make sure you created it first: `python -m venv venv`
- Click "Enter interpreter path..." and browse to it

---

## 🎮 Running the Bot in VS Code

### Option 1: Using the Integrated Terminal

1. **Open Terminal**: View → Terminal (`` Ctrl+` ``)
2. **Activate virtual environment**:
   ```bash
   # Windows
   venv\Scripts\activate
   
   # Mac/Linux
   source venv/bin/activate
   ```
3. **Run commands**:
   ```bash
   python main_bot.py --full-analysis
   ```

**Pro Tip**: Terminal automatically activates venv if configured correctly!

### Option 2: Using Run Configurations (F5)

We've provided pre-configured launch configurations:

1. **Open main_bot.py**
2. **Press F5** (or Run → Start Debugging)
3. **Select configuration**:
   - "Full Analysis" - Complete market analysis
   - "Full Analysis (1h interval)" - Hourly data
   - "Watch EUR/USD" - Monitor EUR/USD in real-time
   - "Watch AAPL" - Monitor Apple stock
   - "Quick Analysis (No Training)" - Fast analysis

**View all configurations**: Run → Open Configurations

### Option 3: Using Tasks (Ctrl+Shift+B)

1. Press **Ctrl+Shift+P**
2. Type "Run Task"
3. Select a task:
   - Run Full Analysis
   - Run Full Analysis (1h)
   - Watch EUR/USD
   - Install Requirements
   - Update Requirements

**Keyboard shortcut**: Ctrl+Shift+B runs default task

### Option 4: Using the Run Button

1. Open `main_bot.py`
2. Click the **▶️ play button** in top-right corner
3. Bot runs with default settings

---

## 🐛 Debugging in VS Code

### Setting Breakpoints

1. Click left of line number (red dot appears)
2. Press F5 to start debugging
3. Code pauses at breakpoint

### Debug Controls

- **F5**: Continue
- **F10**: Step Over (next line)
- **F11**: Step Into (enter function)
- **Shift+F11**: Step Out (exit function)
- **Shift+F5**: Stop debugging
- **Ctrl+Shift+F5**: Restart debugging

### Debug Panel Features

**Variables Panel**: See all variables and their values
**Watch Panel**: Monitor specific expressions
**Call Stack**: See function call hierarchy
**Debug Console**: Execute Python code while paused

**Pro Tip**: Hover over variables to see their values!

### Debug Example

```python
# Set breakpoint on this line
df = fetcher.fetch_forex_data("EUR/USD")

# When stopped here, use Debug Console:
>>> df.head()  # View data
>>> df['close'].iloc[-1]  # Check last price
>>> len(df)  # See data length
```

---

## 📂 File Navigation

### Quick Open
- **Ctrl+P**: Quick file open
  - Type filename to open
  - Type `:line_number` to go to line
  - Type `@symbol` to find symbols

### Symbol Navigation
- **Ctrl+Shift+O**: Go to symbol in file
- **Ctrl+T**: Go to symbol in workspace

### Definition Navigation
- **F12**: Go to definition
- **Alt+F12**: Peek definition (inline)
- **Shift+F12**: Find all references

### Breadcrumbs
- Top of editor shows file path
- Click to navigate project structure

---

## 🔍 Search and Replace

### Find in File
- **Ctrl+F**: Find
- **Ctrl+H**: Replace
- **Alt+Enter**: Select all occurrences

### Find in Folder
- **Ctrl+Shift+F**: Search across files
- **Ctrl+Shift+H**: Replace across files

### Advanced Search
- Use regex: Click `.*` button
- Case sensitive: Click `Aa` button
- Whole word: Click `Ab|` button

---

## 🎨 Code Editing Features

### IntelliSense (Auto-complete)
- Trigger: Start typing or press **Ctrl+Space**
- Shows: Methods, parameters, documentation
- Navigate: Arrow keys, Enter to accept

### Code Snippets
Type these and press Tab:
- `def` → Function definition
- `class` → Class definition
- `if` → If statement
- `for` → For loop

### Format Code
- **Shift+Alt+F**: Format entire file
- **Ctrl+K Ctrl+F**: Format selection

### Multi-Cursor Editing
- **Alt+Click**: Add cursor
- **Ctrl+Alt+Down/Up**: Add cursor below/above
- **Ctrl+D**: Select next occurrence
- **Ctrl+Shift+L**: Select all occurrences

### Comment Code
- **Ctrl+/**: Toggle line comment
- **Shift+Alt+A**: Toggle block comment

---

## 🧪 Testing Support

### Run Tests
1. Open Test Explorer (beaker icon in sidebar)
2. Configure test framework (pytest)
3. Click play button to run tests

### Create Test
```python
# test_example.py
def test_data_fetcher():
    from data.fetcher import DataFetcher
    fetcher = DataFetcher()
    df = fetcher.fetch_forex_data("EUR/USD", lookback=10)
    assert not df.empty
    assert len(df) <= 10
```

---

## 📊 View Multiple Files

### Split Editor
- **Ctrl+\\**: Split editor
- **Ctrl+1/2/3**: Focus editor group
- Drag tabs to split

### Editor Groups
- Arrange in grid: View → Editor Layout
- Move file: Drag tab to group

### Side by Side Comparison
- Select two files in Explorer
- Right-click → "Compare Selected"

---

## 🔧 Terminal Tips

### Multiple Terminals
- **Ctrl+Shift+`**: Create new terminal
- **Ctrl+Tab**: Switch between terminals
- Click `+` icon in terminal panel

### Split Terminal
- Click split icon (⧉) in terminal panel
- **Ctrl+Shift+5**: Split terminal

### Terminal Commands History
- **Up/Down arrows**: Previous commands
- **Ctrl+R**: Search command history

### Clear Terminal
- Type `clear` (Mac/Linux)
- Type `cls` (Windows)
- **Ctrl+K**: Clear terminal (VS Code)

---

## 🎯 Workspace Settings

### Project-Specific Settings

Create `.vscode/settings.json` (already included):
```json
{
    "python.defaultInterpreterPath": "${workspaceFolder}/venv/bin/python",
    "python.terminal.activateEnvironment": true,
    "editor.formatOnSave": true
}
```

### User Settings vs Workspace Settings
- **User Settings**: Apply to all projects
- **Workspace Settings**: Only this project
- Access: File → Preferences → Settings (Ctrl+,)

---

## 🚀 Productivity Extensions

### Recommended Extensions

1. **Python** (Microsoft) ⭐ Essential
   - IntelliSense, debugging, linting

2. **Pylance** (Microsoft) ⭐ Essential
   - Fast, feature-rich language support

3. **Python Docstring Generator**
   - Auto-generate docstrings
   - Type `"""` and press Enter

4. **autoDocstring**
   - Smart docstring generation

5. **Python Indent**
   - Correct Python indentation

6. **Better Comments**
   - Colorful comment highlighting
   - `# TODO:`, `# FIXME:`, `# NOTE:`

7. **Error Lens**
   - Inline error display

8. **Path Intellisense**
   - Autocomplete file paths

9. **GitLens** (if using Git)
   - Enhanced Git integration

10. **Material Icon Theme**
    - Better file icons

### Install Extensions
1. Ctrl+Shift+X
2. Search extension name
3. Click Install

---

## ⌨️ Essential Keyboard Shortcuts

### General
- `Ctrl+P`: Quick file open
- `Ctrl+Shift+P`: Command palette
- `Ctrl+,`: Settings
- `Ctrl+B`: Toggle sidebar
- `Ctrl+J`: Toggle panel

### Editing
- `Ctrl+X`: Cut line
- `Ctrl+C`: Copy line
- `Ctrl+V`: Paste
- `Ctrl+Z`: Undo
- `Ctrl+Shift+Z`: Redo
- `Alt+Up/Down`: Move line up/down
- `Shift+Alt+Up/Down`: Copy line up/down

### Navigation
- `Ctrl+G`: Go to line
- `F12`: Go to definition
- `Ctrl+Tab`: Switch files
- `Alt+Left/Right`: Navigate back/forward

### Terminal
- `` Ctrl+` ``: Toggle terminal
- `Ctrl+Shift+``: New terminal

### Running
- `F5`: Start debugging
- `Ctrl+F5`: Run without debugging
- `Shift+F5`: Stop

---

## 🎨 Customization

### Color Theme
1. File → Preferences → Color Theme (Ctrl+K Ctrl+T)
2. Popular themes:
   - Dark+ (default)
   - Monokai
   - Dracula
   - One Dark Pro

### Font Size
1. Ctrl+, (Settings)
2. Search "font size"
3. Adjust Editor: Font Size

### Keyboard Shortcuts
1. File → Preferences → Keyboard Shortcuts (Ctrl+K Ctrl+S)
2. Search shortcut
3. Click pencil icon to change

---

## 🔄 Git Integration (Optional)

### Initialize Git
```bash
git init
git add .
git commit -m "Initial commit"
```

### Source Control Panel
- **Ctrl+Shift+G**: Open Source Control
- View changes, stage files, commit

### Git Commands
- **Commit**: Ctrl+Shift+G → Enter message → Commit
- **Push**: Click "..." → Push
- **Pull**: Click "..." → Pull

---

## 🆘 Troubleshooting

### Python Not Found
1. Ctrl+Shift+P → "Python: Select Interpreter"
2. Choose correct Python/venv
3. Restart VS Code

### Terminal Not Activating Venv
1. Check `.vscode/settings.json`:
   ```json
   "python.terminal.activateEnvironment": true
   ```
2. Restart terminal: Kill and create new

### Import Errors
1. Make sure venv is activated
2. Check PYTHONPATH in settings
3. Reload window: Ctrl+Shift+P → "Reload Window"

### Linting Errors
1. Install pylint: `pip install pylint`
2. Or disable: Settings → Python › Linting: Enabled

### IntelliSense Not Working
1. Reload window: Ctrl+Shift+P → "Reload Window"
2. Clear cache: Delete `~/.vscode/extensions`
3. Reinstall Pylance extension

---

## 💡 Pro Tips

1. **Use Command Palette**: Ctrl+Shift+P for everything
2. **Learn one shortcut per day**: Become more efficient
3. **Customize keybindings**: Make VS Code work for you
4. **Use Zen Mode**: View → Appearance → Zen Mode (distraction-free)
5. **Peek Definition**: Alt+F12 instead of F12 (stay in context)
6. **Use Breadcrumbs**: Navigate code structure easily
7. **Pin frequently used files**: Right-click tab → Pin
8. **Use Workspaces**: Save multiple folder configurations

---

## 📚 Additional Resources

- **VS Code Docs**: https://code.visualstudio.com/docs
- **Python in VS Code**: https://code.visualstudio.com/docs/python/python-tutorial
- **Keyboard Shortcuts PDF**: Help → Keyboard Shortcut Reference
- **Interactive Playground**: Help → Interactive Playground

---

## 🎓 Learning Path

1. ✅ **Week 1**: Basic navigation and editing
2. ✅ **Week 2**: Running and debugging
3. ✅ **Week 3**: Advanced features (multi-cursor, refactoring)
4. ✅ **Week 4**: Customization and extensions

**Practice makes perfect!** The more you use VS Code, the more productive you'll become.

---

**Happy coding! 🚀**
