import requests
import json
import os
import time
import uuid
from dotenv import load_dotenv
from pathlib import Path


def upload_import_package(server_url: str, session_id: str, import_path: str):
    api_url = server_url + "/public/core/v3/import/package"
    headers = {
        "INFA-SESSION-ID": session_id
    }
    with open(import_path, "rb") as f:
        files = {"package": (import_path, f)}
        response = requests.post(api_url, headers=headers, files=files)

    import_job_id = 0
    if response.status_code == 200 or response.status_code == 201:
        print(f"[V] Import package uploaded successfully")
        data = json.loads(response.content)
        import_job_id = data['jobId']
        return import_job_id
    else:
        print(f"Error {response.status_code}: {response.text}")
        return import_job_id
    

def create_import_job(server_url: str, session_id: str,  ic_import_job_id: str, import_job_name: str, list_object_id: list, conflict_resolution: str):
    api_url = server_url + "/public/core/v3/import/" + ic_import_job_id
    headers = {
        "Content-Type": "application/json",
        "INFA-SESSION-ID": session_id
    }
    payload = {   
        "name" : import_job_name,
        "importSpecification" : {
            "defaultConflictResolution" : conflict_resolution,
            "includeObjects" : [            
               i for i in list_object_id
            ]
        }
    }

    response = requests.post(api_url, headers=headers, json=payload)

    import_status = ""
    if response.status_code == 200:
        data = json.loads(response.content)
        import_status = data['status']["state"]
        print(f"[V] Import Job created successfully")
        return import_status
    else:
        print(f"Error {response.status_code}: {response.text}")
        return import_status


# --- Entry point ---
if __name__ == "__main__":

    # === 0. set up variables  ===
    print("\n=== 0. set up variables  ===")
    env_path = Path('./env/.env')
    load_dotenv(dotenv_path=env_path)
    IC_USERNAME = os.getenv("IC_USERNAME")
    IC_PASSWORD = os.getenv("IC_PASSWORD")
    IC_LOGIN_URL = os.getenv("IC_LOGIN_URL")

    IC_SERVER_URL = ""
    IC_SESSION_ID = ""

    IC_OBJECT_NAME = "mt_startRUN_custom_scen"
    IMPORT_CONFLICT_RESOLUTION = "OVERWRITE"
    IMPORT_JOB_NAME = "test_import_" + str(uuid.uuid4())
    IMPORT_FOLDER = "./imports/"
    IMPORT_FILE = IC_OBJECT_NAME + ".zip"


    # === 1. Authorization ===
    print("\n=== 1. Authorization ===")
    auth_payload = {
        "@type": "login",
        "username": IC_USERNAME,
        "password": IC_PASSWORD
    }
    auth_response = requests.post(IC_LOGIN_URL, json=auth_payload)

    if auth_response.status_code == 200:
        response_data = json.loads(auth_response.content)
        IC_SERVER_URL = response_data['serverUrl']
        IC_SESSION_ID = response_data['icSessionId']
        print("[V] Authentication successful")
        print(f"IC_SERVER_URL: {IC_SERVER_URL}")
        print(f"IC_SESSION_ID: {IC_SESSION_ID}")

        # === 2. Upload Import Package === 
        print("\n=== 2. Upload Import Package === ")
        import_path = IMPORT_FOLDER + IMPORT_FILE
        ic_import_job_id = upload_import_package(IC_SERVER_URL, IC_SESSION_ID, import_path)
        print(f"ic_import_job_id: {ic_import_job_id}")

        # === 3. Create Import Job ===
        print("\n=== 3. Create Import Job ===")
        list_object_id = ["7ytNIqjKXwyj8bk2hGNzYI","6PVdmuxEuSSfi0FCb1CsS1"]
        ic_import_job_status = create_import_job(IC_SERVER_URL, IC_SESSION_ID, ic_import_job_id, IMPORT_JOB_NAME, list_object_id, IMPORT_CONFLICT_RESOLUTION)
        print(f"ic_import_job_status: {ic_import_job_status}")
        time.sleep(5)

        # === 4. Checking Export Job status ===
        print("\n>> Import should be completed")

    else:
        print("Authentication denied!")
        print(f"status_code: {auth_response.status_code}")
        print(auth_response.text)