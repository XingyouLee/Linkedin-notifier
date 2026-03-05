from time import time
import os
from pathlib import Path
import requests
from dotenv import load_dotenv
import time
load_dotenv(Path(__file__).with_name('.env'))

# time1 = time.time()
# API_KEY = os.getenv("GMN_API_KEY")
# payload = {
#   "model": "gpt-5.3-codex-xhigh",
#   "input": [
#     {"type": "message", "role": "user",
#      "content": [{"type": "input_text", "text": "你是什么模型, 模型版本是多少"}]}
#   ]
# }

# r = requests.post(
#   "https://gmn.chuangzuoli.com/v1/responses",
#   headers={
#     "Content-Type": "application/json",
#     "Authorization": f"Bearer {API_KEY}",
#   },
#   json=payload,
#   timeout=60,
# )

# r.raise_for_status()
# print(r.json())
# time2 = time.time()
# print(time2 - time1)





time1 = time.time()
API_KEY = os.getenv("YUAN_API_KEY")
payload = {
  "model": "gpt-5.3-codex-xhigh",
  "input": [
    {"type": "message", "role": "user",
     "content": [{"type": "input_text", "text": "你是什么模型, 模型版本是多少"}]}
  ]
}

r = requests.post(
  "https://api.mcxhm.cn/v1/responses",
  headers={
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}",
  },
  json=payload,
  timeout=60,
)

r.raise_for_status()
print(r.json())
time2 = time.time()
print(time2 - time1)