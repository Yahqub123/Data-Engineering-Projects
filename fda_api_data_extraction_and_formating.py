import requests
import json, os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import zipfile
import io
import datetime, time

todayDate = datetime.date.today()
todayDateISO = todayDate.isoformat()

prevDay = todayDate - datetime.timedelta(days=1)
prevDayISO = prevDay.isoformat()

Storage_directory = "C:/Users/yahqu/Downloads"
All_file_destinations = []


def GetRequested_Data():
    requestURL = 'https://api.fda.gov/download.json'
    try:
        response = requests.request("GET", requestURL)
        # json.dump(requestURL)
        data = response.json()
        desiredData = data[
            "results"]  # gettting the desired data. already hashed it so we have the result value returned.
    except Exception as err:
        print(err)
    return desiredData  # return the requested data


def GetAll_DisplayNames(Data, i, j):
    display_names = []
    for k in Data[i][j]["partitions"]:
        display_names.append(k["display_name"])
    return display_names


def CleanDisplay_Names(display_names):
    cleaned_display_names = []
    for name in display_names:
        cleaned_name, sep, removed_text = name.partition("(")
        cleaned_display_names.append(cleaned_name)
    return cleaned_display_names


def GetUnique_DisplayNames(cleaned_display_names):
    Unique_display_names = np.unique(cleaned_display_names)  # numpy unique to get unique display names
    Unique_display_names = list(Unique_display_names)
    return Unique_display_names


def CreateRoot_folders(Unique_display_names, result_folder_name):
    for i in Unique_display_names:
        try:
            if i[0] == "/":
                parent_directory = Storage_directory
                path = parent_directory + result_folder_name + i  # i in this case is the unique display name
                os.makedirs(path, exist_ok=True)
            elif len(i) > 8:
                parent_directory = Storage_directory
                path = parent_directory + result_folder_name + "/" + i  # i in this case is the unique display name
                os.makedirs(path, exist_ok=True)
            # else:
            #     parent_directory = Storage_directory
            #     Directory_name_Year = str(i[0:4])
            #     Directory_name_quater = str(i[5:7])
            #     Full_directory_name = parent_directory + result_folder_name + "/" + Directory_name_Year + "/" + Directory_name_quater  # Cocatinating the year and quater of the data to be retrieved with the storage directory specified
            #     path = Full_directory_name
            #     os.makedirs(path, exist_ok=True)
        except Exception as err:
            print(err)


def Download_Data_And_Place_In_Created_Folders(data, i, j, result_folder_name):
    parent_directory = Storage_directory
    for k in data[i][j]["partitions"]:
        cleaned_display_name, sep, removed_text = k['display_name'].partition("(")
        LinkToDataFile = k["file"]
        try:
            if cleaned_display_name[0] == "/":
                finalpath = parent_directory + result_folder_name + cleaned_display_name
                finalpath = finalpath.rstrip(' ')
                response = requests.get(LinkToDataFile)  # making a response to get the data from the specified URL
                z = zipfile.ZipFile(io.BytesIO(response.content))
                z.extractall(finalpath)

                All_file_destinations.append(finalpath)  # keeping a list of all our file paths.

            elif len(cleaned_display_name) > 8:
                finalpath = parent_directory + result_folder_name + "/" + cleaned_display_name
                finalpath = finalpath.rstrip(' ')
                response = requests.get(LinkToDataFile)
                z = zipfile.ZipFile(io.BytesIO(response.content))
                z.extractall(finalpath)

                All_file_destinations.append(finalpath)  # keeping a list of all our file paths.

            # else:
            #     finalpath = parent_directory + result_folder_name + "/" + str(cleaned_display_name[0:4]) + "/" + str(
            #         cleaned_display_name[5:7])
            #     finalpath = finalpath.rstrip(' ')
            #     response = requests.get(LinkToDataFile)
            #     z = zipfile.ZipFile(io.BytesIO(response.content))
            #     z.extractall(finalpath)
            #     All_file_destinations.append(finalpath) #keeping a list of all our file paths.

        except Exception as err:
            print(err)


def Convert_Json_ResultFiles_To_Parquet(results_directory, Convert):
    if Convert == True:
        for root, subdirs, files in os.walk(results_directory):
            for file in files:
                if file.endswith('.json'):

                    json_path = root + "//" + file

                    with open(json_path) as project_file:
                        data = json.load(project_file)

                    df = pd.DataFrame(data['results'])

                    for issueColumns in ["reportduplicate", "drug", "animal"]:
                        if issueColumns in df.columns:
                            df[issueColumns] = df[issueColumns].astype(str)

                    # write the DataFrame to a Parquet file
                    parquet_path = json_path.rstrip(".json") + '.parquet'

                    try:
                        df.to_parquet(parquet_path, engine='pyarrow')
                        os.remove(json_path)  # delete the original JSON file

                    except pa.ArrowInvalid as InvalidArrow:
                        word, sep, remainder = InvalidArrow.args[1].partition("column ")
                        column_name, sep, remainder = remainder.partition(" with")
                        df[column_name] = df[column_name].astype(str)  # Changing the column with the error to string
                        try:
                            df.to_parquet(parquet_path, engine='pyarrow')
                            os.remove(json_path)  # delete the original JSON file

                        except pa.ArrowInvalid as DidNotConvertToString:
                            df[column_name] = df[column_name].map(str)
                            print(df.dtypes)
                            df.to_parquet(parquet_path, engine='pyarrow')
                            os.remove(json_path)  # delete the original JSON file

                    except Exception as err:
                        if ("Cannot write struct type" in err.args[0]):
                            word, sep, remainder = err.args[0].partition("'")
                            column_name, sep, remainder = remainder.partition("'")
                            df[column_name] = df[column_name].apply(lambda x: 'dummy')
                            df.to_parquet(parquet_path, engine='pyarrow')
                            os.remove(json_path)  # delete the original JSON file

                        else:
                            print(err)


def main():
    parent_directory = Storage_directory
    data = GetRequested_Data()
    for i in data.keys():
        for j in data[i].keys():
            result_folder_name = "/Results = " + i + j
            display_names = GetAll_DisplayNames(data, i, j)
            cleaned_display_names = CleanDisplay_Names(display_names)
            Unique_display_names = GetUnique_DisplayNames(cleaned_display_names)
            CreateRoot_folders(Unique_display_names, result_folder_name)
            Download_Data_And_Place_In_Created_Folders(data, i, j, result_folder_name)
            Convert_Json_ResultFiles_To_Parquet(parent_directory + result_folder_name,Convert=True)  # Change to false if you do not want to convert the json files to parquet


main()

