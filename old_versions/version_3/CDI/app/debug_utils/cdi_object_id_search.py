import requests
import json
import os
import time
import csv
import logging
from dotenv import load_dotenv
from pathlib import Path


# === 0. set up global variables and logging settings ===
print("\n=== 0. set up global variables and logging settings  ===")
# path to current file
current_path = Path(__file__)
module_path = f"{current_path.parent.parent.parent}"
module_path = module_path.replace("\\", "/")
print(f"module_path: {module_path}")

#DEV ENV
env_path = Path(f'{module_path}/env/.env_dev')
load_dotenv(dotenv_path=env_path)
IC_USERNAME = os.getenv("IC_USERNAME")
IC_PASSWORD = os.getenv("IC_PASSWORD")
IC_LOGIN_URL = os.getenv("IC_LOGIN_URL")

#CI_CD SETTINGS
CI_CD_SESSION_ID = str(int( time.time_ns() / 1000 ))
CI_CD_DIRECTION = "dev_to_qa"
CI_CD_TASK_PATH = f"{module_path}/ci_cd_task/{CI_CD_DIRECTION}/" 

# ========================================================================



def ic_authentication(login: str, password: str):
    auth_payload = {
        "@type": "login",
        "username": login,
        "password": password
    }
    ic_server_url = ""
    ic_session_id = ""
    auth_response = requests.post(IC_LOGIN_URL, json=auth_payload)
    if auth_response.status_code == 200:
        response_data = json.loads(auth_response.content)
        ic_server_url = response_data['serverUrl']
        ic_session_id = response_data['icSessionId']
        print("Authentication successful")
    else:
        print(f"status_code: {auth_response.status_code}")
        print(auth_response.text)
        raise Exception("Authentication denied!")
    return (auth_response.status_code,ic_server_url,ic_session_id)



def get_object_list_to_export(ci_cd_task_path: str):
    # Get first csv-file
    csv_file = next((f for f in os.listdir(ci_cd_task_path) if f.endswith('.csv')), None)
    if not csv_file:
        raise FileNotFoundError("The CSV file was not found in the specified directory.")
    else:
        print(f"ci_cd_task file: {csv_file}")
    file_path = os.path.join(ci_cd_task_path, csv_file)
    list_object = []

    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')  # by default delimiter=','
        next(reader)  # skip headers
        for row in reader:
            if row:  # check if row is not empty
                list_object.append(row)
    return list_object



def get_all_objects_by_type(server_url: str, session_id: str, type: str):
    api_url = server_url + f"/public/core/v3/objects?q=type=='{type}'"
    headers = {
        "INFA-SESSION-ID": session_id
    }
    response = requests.get(api_url, headers=headers)

    list_of_objects = dict()
    if response.status_code == 200:
        list_of_objects = json.loads(response.content)
        return list_of_objects
    else:
        print(f"Error {response.status_code}: {response.text}")
        return list_of_objects


def crete_object_collection(source_object_list: str):
    object_collection = {obj["path"]: obj["id"] for obj in source_object_list["objects"]}
    return object_collection


# --- Entry point ---
if __name__ == "__main__":

    # === 1. Authorization ===
    print("\n=== 1. Authorization ===")
    auth_response_code, ic_server_url, ic_session_id = ic_authentication(IC_USERNAME, IC_PASSWORD)
    print(f"ic_server_url: {ic_server_url}")
    print(f"ic_session_id: {ic_session_id}")


    # === 2. Get list of objects to export-import ===
    print("\n=== 2. Get list of objects to export-import ===")
    list_object_to_export = get_object_list_to_export(CI_CD_TASK_PATH)

    # === 3. Create cdi_objects collection ===
    print("\n=== 3. Create cdi_objects collection ===")
    cdi_object_collection = dict()

    print("add Mappings")
    mapping_list = get_all_objects_by_type(ic_server_url, ic_session_id, 'Mapping')
    mapping_collection = crete_object_collection(mapping_list)
    cdi_object_collection.update(mapping_collection)

    print("add Mapping Tasks")
    mt_list = get_all_objects_by_type(ic_server_url, ic_session_id, 'MTT')
    mt_collection = crete_object_collection(mt_list)
    cdi_object_collection.update(mt_collection)

    print("add TaskFlows")
    tf_list = get_all_objects_by_type(ic_server_url, ic_session_id, 'TASKFLOW')
    tf_collection = crete_object_collection(tf_list)
    cdi_object_collection.update(tf_collection)

    print("\n=== 4. Search id for objects ====")
    map_object_to_export = dict()
    for row in list_object_to_export:
        obj_path = f"{row[2]}/{row[3]}"
        obj_id = cdi_object_collection.get(obj_path, 'not found')
        map_object_to_export[obj_path] = (row[3], obj_id)
    print(f"map_object_to_export: \n {map_object_to_export}")

    for obj_name, obj_id in map_object_to_export.values():
        if obj_id == 'not found':
            raise Exception("[Error]: For some objects the ID was not found, check the logs for the full list")



