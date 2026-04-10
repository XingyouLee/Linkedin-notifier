import requests

url = "https://9985678.xyz/v1/responses"
api_key = "sk-5j0rKUjMt3FgaIRoDlftaK8rIGKuHVHL4hI4sVNe3LyOy4nt"

resp = requests.post(
url,
headers={
"Content-Type": "application/json",
"Authorization": f"Bearer {api_key}",
},
json={
"model": "moonshotai/kimi-k2.5",
"input": "你好"
},
timeout=60,
)

data = resp.json()
print(data)


# from time import time
# import os
# from pathlib import Path
# import requests
# from dotenv import load_dotenv
# import time
# load_dotenv(Path(__file__).with_name('.env'))


# time1 = time.time()
# API_KEY = os.getenv("_998_API_KEY")
# payload = {
#   "model": "kimi-k2.5",
#   "input": [
#     {"type": "message", "role": "user",
#      "content": [{"type": "input_text", "text": "你是什么模型"}]}
#   ]
# }

# r = requests.post(
#   "https://9985678.xyz/v1/responses",
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




# # time1 = time.time()
# # API_KEY = os.getenv("GMN_API_KEY")
# # payload = {
# #   "model": "gpt-5.4",
# #   "input": [
# #     {"type": "message", "role": "user",
# #      "content": [{"type": "input_text", "text": "你是什么模型"}]}
# #   ]
# # }

# # r = requests.post(
# #   "https://gmn.chuangzuoli.com/v1/responses",
# #   headers={
# #     "Content-Type": "application/json",
# #     "Authorization": f"Bearer {API_KEY}",
# #   },
# #   json=payload,
# #   timeout=60,
# # )

# # r.raise_for_status()
# # print(r.json())
# # time2 = time.time()
# # print(time2 - time1)





# # time1 = time.time()
# # API_KEY = os.getenv("YUAN_API_KEY")
# # payload = {
# #   "model": "gpt-5.3-codex-xhigh",
# #   "input": [
# #     {"type": "message", "role": "user",
# #      "content": [{"type": "input_text", "text": "你是什么模型, 模型版本是多少"}]}
# #   ]
# # }

# # r = requests.post(
# #   "https://api.mcxhm.cn/v1/responses",
# #   headers={
# #     "Content-Type": "application/json",
# #     "Authorization": f"Bearer {API_KEY}",
# #   },
# #   json=payload,
# #   timeout=60,
# # )

# # r.raise_for_status()
# # print(r.json())
# # time2 = time.time()
# # print(time2 - time1)


# # time1 = time.time()
# # API_KEY = os.getenv("XCODE_API_KEY")
# # payload = {
# #   "model": "gpt-5.4",
# #   "input": [
# #     {"type": "message", "role": "user",
# #      "content": [{"type": "input_text", "text": "你是什么模型, 模型版本是多少"}]}
# #   ]
# # }

# # r = requests.post(
# #   "https://xcode.best/v1/responses",
# #   headers={
# #     "Content-Type": "application/json",
# #     "Authorization": f"Bearer {API_KEY}",
# #   },
# #   json=payload,
# #   timeout=60,
# # )

# # r.raise_for_status()
# # print(r.json())
# # time2 = time.time()
# # print(time2 - time1)


# time1 = time.time()
# API_KEY = "sk-V7SMD8Ieid4xclf4GifkWyUx6fMgUi1XQaWbLRvjiQkSQqGj"
# payload = {
#   "model": "kimi-k2.5",
#   "input": [
#     {"type": "message", "role": "user",
#      "content": [{"type": "input_text", "text": "你好"}]}
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