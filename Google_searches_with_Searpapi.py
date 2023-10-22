'''In this program I am making an api request with serpapi to make google searches.
I shall also be using try and except clauses in python.'''

import html_to_json
import json
import pandas as pd
import requests

#Link of what you want to search using search api.
urlLink = "https://www.nike.com/t/free-metcon-4-training-shoes-KX41Bv/CT3886-301?nikemt=true&cp=38647199687_search_%7CPRODUCT_GROUP%7CGOOGLE%7C71700000101429394%7CGG_Evergreen_Shopping_Shoes_AllShoes%7C%7Cc&exp=2011&gclsrc=aw.ds&&gclid=CjwKCAiAkrWdBhBkEiwAZ9cdcG7uW3qP3yZZIUe8oysRg4raRJLgiCGHfPXxeNCBlp908KAIcQgWcxoCF1kQAvD_BwE&gclsrc=aw.ds"

response = requests.get(url=urlLink)
response.encoding = 'ISO-8859-1'
print("The data's encoding is:",response.encoding)
print("\nstatusCode:", response.status_code)
#print("\nThe respose headers are:",response.headers)
#print("\nThe response content:",response.content)
#print(response.text)


data_in_json = html_to_json.convert(response.text)
print(data_in_json.keys())
#converting it from a list to dictionary

data = data_in_json['html']
actualData = data[0]

print("Here are the keys to the returned data:", actualData.keys())

################################## Now We access our Data!! Yay!!!#########################
#print(actualData['body'][0])
count = 1
for i in actualData['body'][0].keys():
    if i == 'script':
        print(len(actualData['body'][0][i]))
        #print("Key==", i, "\n\n\n\n", actualData['body'][0][i], "\n")
        #print(actualData['body'][0][i][7])
        # for j in actualData['body'][0][i]:
        #     try:
        #         print(j['_attributes'])
        #     except:
        #         print("This is an exception")
        #         print(j['_value'])
        #     count = count + 1

# for i in data_in_json['html']:
#     print(i)


for i in actualData['body'][0].keys():
    if i == 'script':
        for line in actualData['body'][0][i]:
            #print(line,"\n\n\n")
            if len(line.keys()) == 2:
                #print(line["_attributes"],"\n\n")
                if line["_attributes"]['type'] == 'application/json':
                    #print(line["_value"])
                    json_object = json.loads(line["_value"])

print(json_object)





