import json

import requests
#
# url = "http://localhost:6677/stream_chat"
# message = "Hello, how are you?"
# data = {"content": message}
#
# headers = {"Content-type": "application/json"}
#
# with requests.post(url, data=json.dumps(data), headers=headers, stream=True) as r:
#     for chunk in r.iter_content(1024):
#         print(chunk)




def get_stream(query: str):
    s = requests.Session()
    with s.post(
        "http://localhost:8000/chat",
        stream=True,
        json={"text": query}
    ) as r:
        for line in r.iter_content():
            print(line.decode("utf-8"), end="")



if __name__ == '__main__':
    get_stream("'It's 2030 now. How many years ago did Ataturk become president.")
