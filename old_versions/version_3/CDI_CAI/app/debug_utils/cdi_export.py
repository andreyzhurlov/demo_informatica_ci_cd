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

#DEV ENV
env_path = Path('./././env/.env_dev')
load_dotenv(dotenv_path=env_path)
IC_USERNAME = os.getenv("IC_USERNAME")
IC_PASSWORD = os.getenv("IC_PASSWORD")
IC_LOGIN_URL = os.getenv("IC_LOGIN_URL")

#CI_CD SETTINGS
CI_CD_SESSION_ID = str(int( time.time_ns() / 1000 ))
CI_CD_DIRECTION = "dev_to_qa"
CI_CD_TASK_PATH = f"./././ci_cd_task/{CI_CD_DIRECTION}/" 

LOG_CI_CD_SESSION_FOLDER = "./././log/log_ci_cd_session/"
EXPORT_FOLDER = f"./././export_to_import/{CI_CD_DIRECTION}/"
LOG_EXPORT_FOLDER = "./././log/log_export/"


### set up logging ###
# Specify the directory and name of the log file
log_dir = LOG_CI_CD_SESSION_FOLDER
log_file = f"{CI_CD_SESSION_ID}.log"

# Create the directory if it does not exist
os.makedirs(log_dir, exist_ok=True)

# Full path to the log file
log_path = os.path.join(log_dir, log_file)

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s] %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler(log_path)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

rootLogger.info(f">> CI_CD_SESSION_ID: {CI_CD_SESSION_ID}")
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
        rootLogger.info("Authentication successful")
    else:
        rootLogger.info(f"status_code: {auth_response.status_code}")
        rootLogger.info(auth_response.text)
        raise Exception("Authentication denied!")
    return (auth_response.status_code,ic_server_url,ic_session_id)


def get_object_list_to_export(ci_cd_task_path: str):
    # Get first csv-file
    csv_file = next((f for f in os.listdir(ci_cd_task_path) if f.endswith('.csv')), None)
    if not csv_file:
        raise FileNotFoundError("The CSV file was not found in the specified directory.")
    file_path = os.path.join(ci_cd_task_path, csv_file)
    list_object = []

    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')  # by default delimiter=','
        next(reader)  # skip headers
        for row in reader:
            if row:  # check if row is not empty
                list_object.append((row[0],row[1]))
    return list_object



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
                "includeDependencies": False
            }
        ]
    }
    response = requests.post(api_url, headers=headers, json=payload)

    if response.status_code == 200:
        data = json.loads(response.content)
        export_id = data['id']
        return export_id
    else:
        rootLogger.info(f"Error {response.status_code}: {response.text}")
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
        rootLogger.error(f"Error {response.status_code}: {response.text}")
        return export_status


def load_export_package(server_url: str, session_id: str, export_id: str, export_folder: str, export_file: str):
    if not os.path.exists(export_folder):
        os.makedirs(export_folder)
        rootLogger.info(f">> Export directory created: {export_folder}")
    export_path = export_folder + export_file

    api_url = server_url + "/public/core/v3/export/" + export_id + "/package"
    headers = {
        "INFA-SESSION-ID": session_id
    }
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        with open(export_path, "wb") as f:
            f.write(response.content)
        rootLogger.info(f"[V] Package saved successfully in path '{export_path}'")
        return 1
    else:
        rootLogger.error(f"[X] Error: status {response.status_code}")
        rootLogger.error(response.text)
        return 0


def load_export_log(server_url: str, session_id: str, export_id: str, log_folder: str, log_file: str):
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
        rootLogger.info(f">> Export Log directory created: {log_folder}")
    log_path = log_folder + log_file

    api_url = server_url + "/public/core/v3/export/" + export_id + "/log"
    headers = {
        "INFA-SESSION-ID": session_id
    }
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        with open(log_path, "wb") as f:
            f.write(response.content)
        rootLogger.info(f"[V] Export Log saved successfully in path '{log_path}'")
        return 1
    else:
        rootLogger.error(f"[X] Error: status {response.status_code}")
        rootLogger.error(response.text)
        return 0


def load_export_log(server_url: str, session_id: str, export_id: str, log_folder: str, log_file: str):
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
        rootLogger.info(f">> Export Log directory created: {log_folder}")
    log_path = log_folder + log_file

    api_url = server_url + "/public/core/v3/export/" + export_id + "/log"
    headers = {
        "INFA-SESSION-ID": session_id
    }
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        with open(log_path, "wb") as f:
            f.write(response.content)
        rootLogger.info(f"[V] Export Log saved successfully in path '{log_path}'")
        return 1
    else:
        rootLogger.info(f"[X] Error: status {response.status_code}")
        rootLogger.info(response.text)
        return 0
    

# --- Entry point ---
if __name__ == "__main__":

    # === 1. Authorization ===
    rootLogger.info("\n=== 1. Authorization ===")
    auth_response_code, ic_server_url, ic_session_id = ic_authentication(IC_USERNAME, IC_PASSWORD)
    rootLogger.info(f"ic_server_url: {ic_server_url}")
    rootLogger.info(f"ic_session_id: {ic_session_id}")

    # === 2. Get list of objects to export ===
    rootLogger.info("\n=== 2. Get list of objects to export ===")
    list_object_to_export = get_object_list_to_export(CI_CD_TASK_PATH)
    rootLogger.info(list_object_to_export)

    # === 3. Run Export Job for each object ===
    rootLogger.info("\n=== 3. Run Export Job for each object ===")
    export_session_folder = EXPORT_FOLDER + CI_CD_SESSION_ID + "/"
    LOG_EXPORT_FOLDER_session = LOG_EXPORT_FOLDER + CI_CD_SESSION_ID + "/"

    # Exporting
    k = 0
    for ic_object_id, ic_object_name in list_object_to_export:
        k += 1
        export_job_name = f"{ic_object_name}-{CI_CD_SESSION_ID}"
        rootLogger.info(f"\n(3.{k}) >> export_job_name: {export_job_name}")
        ic_export_job_id = create_export_job(ic_server_url, ic_session_id, export_job_name, ic_object_id)
        rootLogger.info(f"(3.{k}) >> ic_export_job_id: {ic_export_job_id}")
        time.sleep(3)

        # === 4. Checking Export Job status ===
        rootLogger.info(f"\n===  4.{k} Checking Export Job status ===")
        # 10 checks with pause in N sec
        n_attempts = 11
        pause_sec = 3
        ic_export_job_status = ""
        for i in range(1, n_attempts):
            ic_export_job_status = check_export_job_status(ic_server_url, ic_session_id, ic_export_job_id)
            rootLogger.info(f"(4.{k}) >> [{i}] check_export_job_status: {ic_export_job_status}")
            if ic_export_job_status == "SUCCESSFUL":
                break
            time.sleep(pause_sec)

        if ic_export_job_status == "SUCCESSFUL":
            # === 5. Load Export Package ===
            rootLogger.info(f"\n===  5.{k} Load Export Package ===")
            export_file = f"{ic_object_name}-{CI_CD_SESSION_ID}.zip"
            export_folder = export_session_folder 
            status = load_export_package(ic_server_url, ic_session_id, ic_export_job_id, export_folder, export_file)
            if status == 1:
                rootLogger.info(f"(5.{k}) -=[~+~] Package exported successfully [~+~]=-")
            else:
                rootLogger.error(f"(5.{k}) >> Some error occurred during export... ")  
        else:
            rootLogger.warning(" (5.{k}) >> Please check Export Job status later or repeat it...")

        # === 5. Load Export Package ===
        rootLogger.info(f"\n===  6.{k} Load Export Package Log ===")
        log_export_file = f"ex_{ic_object_name}-{CI_CD_SESSION_ID}.txt"
        status = load_export_log(ic_server_url, ic_session_id, ic_export_job_id, LOG_EXPORT_FOLDER_session, log_export_file)
        if status == 1:
            rootLogger.info(f"(6.{k}) [+] Export log saved")
        else:
            rootLogger.error(f"(6.{k}) >> Some error occurred during log saving ... ")  
        rootLogger.info("==========================================================")
        
    rootLogger.info(f"\n=== Export is finished | CI_CD_SESSION_ID: {CI_CD_SESSION_ID} ===")

    