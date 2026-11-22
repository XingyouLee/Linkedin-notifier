from pathlib import Path
import requests
from dotenv import dotenv_values
import time

ENV_PATH = Path(__file__).with_name(".env")
ENV = dotenv_values(ENV_PATH)


time1 = time.time()
API_KEY = ENV.get("NOWCODING_API_KEY")
if not API_KEY:
    raise RuntimeError(f"NOWCODING_API_KEY is not set in {ENV_PATH}")

payload = {
  "model": "gpt-5.6-terra",
  "input": [
    {"type": "message", "role": "user",
     "content": [{"type": "input_text", "text": "你是什么模型"}]}
  ]
}

r = requests.post(
  "https://nowcoding.ai/v1/responses",
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





# time1 = time.time()
# API_KEY = os.getenv("YUAN_API_KEY")
# payload = {
#   "model": "gpt-5.3-codex-xhigh",
#   "input": [
#     {"type": "message", "role": "user",
#      "content": [{"type": "input_text", "text": "你是什么模型, 模型版本是多少"}]}
#   ]
# }

# r = requests.post(
#   "https://api.mcxhm.cn/v1/responses",
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


# time1 = time.time()
# API_KEY = os.getenv("XCODE_API_KEY")
# payload = {
#   "model": "gpt-5.4",
#   "input": [
#     {"type": "message", "role": "user",
#      "content": [{"type": "input_text", "text": "你是什么模型, 模型版本是多少"}]}
#   ]
# }

# r = requests.post(
#   "https://xcode.best/v1/responses",
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
