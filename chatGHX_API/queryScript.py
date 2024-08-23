import requests

#General Query Script, Example input: "What is GHX and what is their net worth"
def getGenQuery(query):
    if isinstance(query, str):
        url = "https://chatghx-api.awsdsi.ghx.com/chat/user_query/"
        request = '''{"user_id":"kg_capstone_1234", "query":"''' + query + '''"}'''
        resp = requests.post(url, request).json()
        return resp

#Description by Name Query Script, Example input: "GHX"
def getQueryDescriptionFromName(name):
    if isinstance(name, str):
        url = "https://chatghx-api.awsdsi.ghx.com/chat/user_query/"
        request = '''{"user_id":"kg_capstone_1234", "query":"Provide a one sentence description of''' + name + '''"}'''
        resp = requests.post(url, request).json()
        return resp
