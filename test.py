import os
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name('.env'))

print("GMN_API_KEY exists?", "GMN_API_KEY" in os.environ)
print("cwd:", os.getcwd())
API_KEY = os.getenv("GMN_API_KEY")
print("API key loaded:", bool(API_KEY))
payload = {
  "model": "gpt-5.2",
  "input": [
    {"type": "message", "role": "user",
     "content": [{"type": "input_text", "text": "Explain BFS in 2 sentences."}]}
  ]
}

r = requests.post(
  "https://gmn.chuangzuoli.com/v1/responses",
  headers={
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}",
  },
  json=payload,
  timeout=60,
)

r.raise_for_status()
print(r.json())