import os
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
import sys

# Load .env
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("ERROR: OPENAI_API_KEY not found in .env")
    sys.exit(1)

client = OpenAI(api_key=api_key)

# Get file path
if len(sys.argv) < 2:
    print("Usage: python ai_refactor.py <file.py>")
    sys.exit(1)

file_path = Path(sys.argv[1])
if not file_path.exists():
    print(f"File {file_path} does not exist")
    sys.exit(1)

# Read file
with open(file_path, "r", encoding="utf-8") as f:
    code = f.read()

# Prompt
prompt = f"""You are a professional Python developer.

Task:
1. Refactor the following Python code to be cleaner, faster, and safer.
2. Add detailed docstrings to all functions explaining arguments, return values, and behavior.
3. Generate unit tests for every function using pytest. Do not change original functionality.
4. Keep the code in Python and mark tests in a separate section clearly.

Here is the code:
'''{code}'''

Output:
- First, the refactored code with docstrings.
- Then, below a clear section called "# UNIT TESTS", include the pytest tests for all functions.
"""

# Call OpenAI API (v1+)
response = client.chat.completions.create(
    model="gpt-3.5-turbo",  # <-- change here
    messages=[{"role": "user", "content": prompt}],
    temperature=0.2
)

refactored_code = response.choices[0].message.content
print(refactored_code)