import requests
url = "https://chatghx-api.awsdsi.ghx.com/chat/user_query/"
request = '''{"user_id":"kg_capstone_1234", "query":"write some gremlin code to ingest a csv into tinkerpops tinkergraph"}'''
print(requests.post(url, request).json())