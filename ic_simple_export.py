import requests
import json
import os
import time
import uuid
from dotenv import load_dotenv
from pathlib import Path


def get_object_id(server_url: str, session_id: str, object_name: str):
    api_url = server_url + "/api/v2/mttask/name/" + object_name
    headers = {
        "icSessionId": session_id
    }
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        data = json.loads(response.content)
        object_id = data['frsGuid']
        return object_id
    else:
        print(f"Error {response.status_code}: {response.text}")
        return 0


def create_export_job(server_url: str, session_id: str, job_name: str, object_id: str):
    api_url = server_url + "/public/core/v3/export"
    headers = {
        "Content-Type": "application/json",
        "INFA-SESSION-ID": session_id
    }
    payload = {
        "name": job_name,
        "objects": [
            {
                "id": object_id,
                "includeDependencies": True
            }
        ]
    }
    response = requests.post(api_url, headers=headers, json=payload)

    if response.status_code == 200:
        data = json.loads(response.content)
        export_id = data['id']
        return export_id
    else:
        print(f"Error {response.status_code}: {response.text}")
        return 0


def check_export_job_status(server_url: str, session_id: str, export_id):
    api_url = server_url + "/public/core/v3/export/" + export_id
    headers = {
        "INFA-SESSION-ID": session_id
    }
    response = requests.get(api_url, headers=headers)

    export_status = ""
    if response.status_code == 200:
        data = json.loads(response.content)
        export_status = data['status']["state"]
        return export_status
    else:
        print(f"Error {response.status_code}: {response.text}")
        return export_status


def load_export_file(server_url: str, session_id: str, export_id: str, export_path: str):
    api_url = server_url + "/public/core/v3/export/" + export_id + "/package"
    headers = {
        "INFA-SESSION-ID": session_id
    }
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        with open(export_path, "wb") as f:
            f.write(response.content)
        print(f"[V] File saved successfully in path '{export_path}'")
        return 1
    else:
        print(f"[X] Error: status {response.status_code}")
        print(response.text)
        return 0



# --- Entry point ---
if __name__ == "__main__":

    # === 0. set up variables  ===
    env_path = Path('./env/.env')
    load_dotenv(dotenv_path=env_path)
    IC_USERNAME = os.getenv("IC_USERNAME")
    IC_PASSWORD = os.getenv("IC_PASSWORD")
    IC_LOGIN_URL = os.getenv("IC_LOGIN_URL")

    IC_SERVER_URL = ""
    IC_SESSION_ID = ""

    IC_OBJECT_NAME = "mt_startRUN_custom_scen"
    EXPORT_JOB_NAME = "test_export_" + str(uuid.uuid4())
    EXPORT_FOLDER = "./exports/"
    EXPOR_FILE = IC_OBJECT_NAME + ".zip"


    # === 1. Authorization ===
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
        print("Authentication successful")
        print(f"IC_SERVER_URL: {IC_SERVER_URL}")
        print(f"IC_SESSION_ID: {IC_SESSION_ID}")

        # === 2. Get Object ID to Export ===
        ic_object_id = get_object_id(IC_SERVER_URL, IC_SESSION_ID, IC_OBJECT_NAME)
        print(f"ic_object_id: {ic_object_id}")

        # === 3. Create Export Job ===
        ic_export_job_id = create_export_job(IC_SERVER_URL, IC_SESSION_ID, EXPORT_JOB_NAME, ic_object_id)
        print(f"ic_export_job_id: {ic_export_job_id}")
        time.sleep(5)

        # === 4. Checking Export Job status ===
        # 10 checks during 1 minute
        n_attempts = 10
        ic_export_job_status = ""
        for i in range(n_attempts):
            ic_export_job_status = check_export_job_status(IC_SERVER_URL, IC_SESSION_ID, ic_export_job_id)
            print(f"[{i}] check_export_job_status: {ic_export_job_status}")
            if ic_export_job_status == "SUCCESSFUL":
                break
            time.sleep(5)

        if ic_export_job_status == "SUCCESSFUL":
            # === 5. Load Export File ===
            export_path = EXPORT_FOLDER + EXPOR_FILE
            status = load_export_file(IC_SERVER_URL, IC_SESSION_ID, ic_export_job_id, export_path)
            if status == 1:
                print("-=[~+~] Export completed successfully [~+~]=-")
            else:
                print(">> Some error occurred during export... ")  
        else:
            print("Please check Export JOb status later or repeat it...")
    else:
        print("Authentication denied!")
        print(f"status_code: {auth_response.status_code}")
        print(auth_response.text)