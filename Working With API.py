import pandas as pd
import requests

#Getting data from this url from stack overflow.
response = requests.get("https://api.stackexchange.com/2.3/posts?pagesize=100&fromdate=1669852800&todate=1671667200&order=desc&sort=activity&site=stackoverflow")


#getting the enconding of the returned response or data
print("The encoding of the data is:",response.encoding)

#getting the status code of the returned data
print("The status code of responsed data is:",response.status_code)

#Getting the headers of the returned data
print("The headers of the response is:",response.headers)

#Getting the content of the data
print("The content of the data is:",response.content)

print("Checking to see if its a JSON file:: ",response.headers["Content-Type"])

#converting the response format to json so it is easier to work with
Data = response.json()

#Getting the keys of the returned data
print("The keys to the data are:",Data.keys())

#getting the keys to each row of the dataset
print("The keys of each row are:", Data["items"], "\n")

                                        #******************Working With the comment "owner" data for real now************
#I am going to create a list to hold the list of names and ids e.t.c the into a dataframe.
Names_of_comment_owners = []
IDs_of_comment_owners = []
user_type_of_comment_owners = []
Links_of_Questions = []
Popularity_of_comments=[]

for i in Data["items"]:
    Name = i["owner"]["display_name"]
    ID = i["owner"]["user_id"]
    User_type = i["owner"]["user_type"]
    Questions = i["link"]
    Popularity = i["owner"]["reputation"]

    #THEN
    Names_of_comment_owners.append(Name)
    IDs_of_comment_owners.append(ID)
    user_type_of_comment_owners.append(User_type)
    Links_of_Questions.append(Questions)
    Popularity_of_comments.append(Popularity)

#Then we create a dictionary to map the values respectiveliley
StackOverflow_Comments_Data = {
    "User_Name": Names_of_comment_owners,
    "User_Id":IDs_of_comment_owners,
    "User_Type": user_type_of_comment_owners,
    "Popularity":Popularity_of_comments,
    "Link_of_Questions":Links_of_Questions
}
}

Results = pd.DataFrame(StackOverflow_Comments_Data)
print(Results)