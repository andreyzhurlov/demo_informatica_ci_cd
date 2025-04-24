import requests
import json
import os
import time
import csv
import logging
from dotenv import load_dotenv
from pathlib import Path


##########################################################################################
# === 0. set up global variables and logging settings ===
print("\n=== 0. set up global variables and logging settings  ===")
# path to current file
current_path = Path(__file__)
module_path = f"{current_path.parent.parent}"
module_path = module_path.replace("\\", "/")
print(f"module_path: {module_path}")

#ENV for Export
env_path = Path(f'{module_path}/env/.env_dev')
load_dotenv(dotenv_path=env_path)
EX_IC_USERNAME = os.getenv("IC_USERNAME")
EX_IC_PASSWORD = os.getenv("IC_PASSWORD")
EX_IC_LOGIN_URL = os.getenv("IC_LOGIN_URL")

#ENV for Import
env_path = Path(f'{module_path}/env/.env_qa')
load_dotenv(dotenv_path=env_path)
IM_IC_USERNAME = os.getenv("IC_USERNAME")
IM_IC_PASSWORD = os.getenv("IC_PASSWORD")
IM_IC_LOGIN_URL = os.getenv("IC_LOGIN_URL")

#CI_CD SETTINGS
CI_CD_SESSION_ID = str(int( time.time_ns() / 1000 ))
CI_CD_DIRECTION = "dev_to_qa"
CI_CD_TASK_PATH = f"{module_path}/ci_cd_task/{CI_CD_DIRECTION}/" 

LOG_CI_CD_SESSION_FOLDER = f"{module_path}/log/log_ci_cd_session/"
EXPORT_FOLDER = f"{module_path}/export_to_import/{CI_CD_DIRECTION}/"
LOG_EXPORT_FOLDER = f"{module_path}/log/log_export/"

IMPORT_CONFLICT_RESOLUTION = "OVERWRITE"
LOG_IMPORT_FOLDER = f"{module_path}/log/log_import/"

### set up logging ###
# Specify the directory and name of the log file
log_dir = LOG_CI_CD_SESSION_FOLDER
log_file = f"{CI_CD_SESSION_ID}.log"

# Create the directory if it does not exist
os.makedirs(log_dir, exist_ok=True)

# Full path to the log file
log_path = os.path.join(log_dir, log_file)

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-7.5s] %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler(log_path)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

rootLogger.info(f">> CI_CD_SESSION_ID: {CI_CD_SESSION_ID}")

##########################################################################################



# EXPORT UTILS ###########################################################################

def ic_authentication(login_url: str, login: str, password: str):
    auth_payload = {
        "@type": "login",
        "username": login,
        "password": password
    }
    ic_server_url = ""
    ic_session_id = ""
    auth_response = requests.post(login_url, json=auth_payload)
    if auth_response.status_code == 200:
        response_data = json.loads(auth_response.content)
        ic_server_url = response_data['serverUrl']
        ic_session_id = response_data['icSessionId']
        rootLogger.info("Authentication successful")
    else:
        rootLogger.info(f"status_code: {auth_response.status_code}")
        rootLogger.info(auth_response.text)
        raise Exception("Authentication denied!")
    return (auth_response.status_code, ic_server_url, ic_session_id)


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
        raise Exception(f"Error {response.status_code}: {response.text}")
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
        rootLogger.info(f"[X] Error: status {response.status_code}")
        rootLogger.info(response.text)
        return 0
    
#########################################################################################  



# IMPORT UTILS ###########################################################################

def upload_import_package(server_url: str, session_id: str, export_to_import_path: str):
    api_url = server_url + "/public/core/v3/import/package"
    headers = {
        "INFA-SESSION-ID": session_id
    }
    rootLogger.info(f"export_to_import_path: {export_to_import_path}")
    with open(export_to_import_path, "rb") as f:
        files = {"package": (export_to_import_path, f)}
        response = requests.post(api_url, headers=headers, files=files)

    import_job_id = 0
    if response.status_code in (200, 201):
        rootLogger.info(f"[V] Import package uploaded successfully")
        data = json.loads(response.content)
        import_job_id = data['jobId']
        return import_job_id
    else:
         rootLogger.error(f"[Error] {response.status_code}: {response.text}")
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
        rootLogger.info(f"[V] Import Job created successfully")
        return import_status
    else:
        rootLogger.error(f"Error {response.status_code}: {response.text}")
        return import_status


def check_import_job_status(server_url: str, session_id: str, import_id):
    api_url = server_url + "/public/core/v3/import/" + import_id
    headers = {
        "INFA-SESSION-ID": session_id
    }
    response = requests.get(api_url, headers=headers)

    status = ""
    if response.status_code == 200:
        data = json.loads(response.content)
        status = data['status']["state"]
        return status
    else:
        rootLogger.error(f"Error {response.status_code}: {response.text}")
        raise Exception(f"Error {response.status_code}: {response.text}")
    return status


def load_import_log(server_url: str, session_id: str, export_id: str, log_folder: str, log_file: str):
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
        rootLogger.info(f">> Import Log directory created: {log_folder}")
    log_path = log_folder + log_file

    api_url = server_url + "/public/core/v3/import/" + export_id + "/log"
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
    
#########################################################################################  



########################################################################################
# --- Entry point ---
if __name__ == "__main__":

    # === 1. Authorization ===
    rootLogger.info("\n=== 1.1 Authorization in Export env ===")
    auth_response_code, ex_ic_server_url, ex_ic_session_id = ic_authentication(EX_IC_LOGIN_URL, EX_IC_USERNAME, EX_IC_PASSWORD)
    rootLogger.info(f"ex_ic_server_url: {ex_ic_server_url}")
    rootLogger.info(f"ex_ic_session_id: {ex_ic_session_id}")

    rootLogger.info("\n=== 1.2 Authorization in Import env ===")
    auth_response_code, im_ic_server_url, im_ic_session_id = ic_authentication(IM_IC_LOGIN_URL, IM_IC_USERNAME, IM_IC_PASSWORD)
    rootLogger.info(f"im_ic_server_url: {im_ic_server_url}")
    rootLogger.info(f"im_ic_session_id: {im_ic_session_id}")

    # === 2. Get list of objects to export ===
    rootLogger.info("\n=== 2. Get list of objects to export ===")
    list_object_to_export = get_object_list_to_export(CI_CD_TASK_PATH)

    # === 7. Create cdi_objects collection ===
    rootLogger.info("\n=== 3. Create cdi_objects collection ===")
    cdi_object_collection = dict()

    rootLogger.info("(3) add Mappings")
    mapping_list = get_all_objects_by_type(ex_ic_server_url, ex_ic_session_id, 'Mapping')
    mapping_collection = crete_object_collection(mapping_list)
    cdi_object_collection.update(mapping_collection)

    rootLogger.info("(3) add Mapping Tasks")
    mt_list = get_all_objects_by_type(ex_ic_server_url, ex_ic_session_id, 'MTT')
    mt_collection = crete_object_collection(mt_list)
    cdi_object_collection.update(mt_collection)

    rootLogger.info("(3) add TaskFlows")
    tf_list = get_all_objects_by_type(ex_ic_server_url, ex_ic_session_id, 'TASKFLOW')
    tf_collection = crete_object_collection(tf_list)
    cdi_object_collection.update(tf_collection)

    rootLogger.info("\n=== 4. Search id for objects ====")
    map_object_to_export = dict()
    for row in list_object_to_export:
        obj_path = f"{row[2]}/{row[3]}"
        obj_id = cdi_object_collection.get(obj_path, 'not found')
        map_object_to_export[obj_path] = (row[3], obj_id)
    rootLogger.info(f"(4) map_object_to_export: \n{map_object_to_export}")

    for obj_name, obj_id in map_object_to_export.values():
        if obj_id == 'not found':
            rootLogger.error("(4) For some objects the ID was not found, check the logs for the full list")
            raise Exception("(4) [Error]: For some objects the ID was not found, check the logs for the full list")

    # === 5. Run Export Job for each object ===
    rootLogger.info("\n=== 5. Run Export - Import Job for each object ===")
    export_session_folder = EXPORT_FOLDER + CI_CD_SESSION_ID + "/"
    log_export_folder_session = LOG_EXPORT_FOLDER + CI_CD_SESSION_ID + "/"
    log_import_folder_session = LOG_IMPORT_FOLDER + CI_CD_SESSION_ID + "/"

    # Exporting
    k = 0
    for ic_object_path, ic_object_metadata in map_object_to_export.items():
        k += 1
        ic_object_name = ic_object_metadata[0]
        ic_object_id = ic_object_metadata[1]
        rootLogger.info(f"\n(5.{k}) >> ic_object_path: {ic_object_path} | ic_object_name: {ic_object_name} | ic_object_id: {ic_object_id}")

        export_job_name = f"{ic_object_name}-{CI_CD_SESSION_ID}"
        rootLogger.info(f"\n(5.{k}) >> export_job_name: {export_job_name}")
        ic_export_job_id = create_export_job(ex_ic_server_url, ex_ic_session_id, export_job_name, ic_object_id)
        rootLogger.info(f"(5.{k}) >> ic_export_job_id: {ic_export_job_id}")
        time.sleep(3)

        # === 6. Checking Export Job status ===
        rootLogger.info(f"\n===  6.{k} Checking Export Job status ===")
        # X checks with pause in N sec
        n_attempts = 11
        pause_sec = 3
        ic_export_job_status = ""
        for i in range(1, n_attempts):
            ic_export_job_status = check_export_job_status(ex_ic_server_url, ex_ic_session_id, ic_export_job_id)
            rootLogger.info(f"(6.{k}) >> [{i}] check ic_export_job_status: {ic_export_job_status}")
            if ic_export_job_status == "SUCCESSFUL":
                break
            time.sleep(pause_sec)

        export_file = f"{ic_object_name}-{CI_CD_SESSION_ID}.zip"
        export_folder = export_session_folder
        export_to_import_path = export_folder + export_file
        if ic_export_job_status == "SUCCESSFUL":
            # === 7. Load Export Package ===
            rootLogger.info(f"\n===  7.{k} Load Export Package ===")
            status = load_export_package(ex_ic_server_url, ex_ic_session_id, ic_export_job_id, export_folder, export_file)
            if status == 1:
                rootLogger.info(f"(7.{k}) -=[~+~] Package exported successfully [~+~]=-")
            else:
                rootLogger.error(f"(7.{k}) >> Some error occurred during export... ")  
        else:
            rootLogger.warning(" (7.{k}) >> Please check Export Job status later or repeat it...")

        # === 8. Load Export Package ===
        rootLogger.info(f"\n===  8.{k} Load Export Package Log ===")
        log_export_file = f"ex_{ic_object_name}-{CI_CD_SESSION_ID}.txt"
        status = load_export_log(ex_ic_server_url, ex_ic_session_id, ic_export_job_id, log_export_folder_session, log_export_file)
        if status == 1:
            rootLogger.info(f"(8.{k}) [+] Export log saved")
        else:
            rootLogger.error(f"(8.{k}) >> Some error occurred during log saving ... ")

        rootLogger.info("==========================================================")

        # === 9. Upload Import Package === 
        rootLogger.info("\n=== 9. Upload Import Package === ")
        ic_import_job_id = upload_import_package(im_ic_server_url, im_ic_session_id, export_to_import_path)
        rootLogger.info(f"(9.{k}) ic_import_job_id: {ic_import_job_id}")
        if ic_import_job_id == 0:
            raise Exception(f"(9.{k}) [Error]: ic_import_job_id is invalid, please check logs")

        # === 10. Create Import Job ===
        rootLogger.info("\n=== 10. Create Import Job ===")
        import_job_name = export_job_name
        list_object_id = [ic_object_id]

        ic_import_job_status = create_import_job(im_ic_server_url, im_ic_session_id, ic_import_job_id, import_job_name, list_object_id, IMPORT_CONFLICT_RESOLUTION)
        rootLogger.info(f"(10) ic_import_job_status: {ic_import_job_status}")
        time.sleep(3)
        
        # === 11. Checking Import Job status ===
        rootLogger.info(f"\n=== 11.{k} Checking Import Job status ===")
        # X checks with pause in N sec
        n_attempts = 15
        pause_sec = 3
        ic_import_job_status = ""
        for i in range(1, n_attempts):
            ic_import_job_status = check_export_job_status(im_ic_server_url, im_ic_session_id, ic_export_job_id)
            rootLogger.info(f"(11.{k}) >> [{i}] check ic_import_job_status: {ic_import_job_status}")
            if ic_import_job_status == "SUCCESSFUL":
                break
            time.sleep(pause_sec)
        
        if ic_import_job_status == "SUCCESSFUL":
            # === 12. Load Import  Log  ===
            rootLogger.info(f"\n=== 12.{k} Load Import  Log ===")
            log_import_file = f"im_{ic_object_name}-{CI_CD_SESSION_ID}.txt"
            status = load_import_log(im_ic_server_url, im_ic_session_id, ic_import_job_id, log_import_folder_session, log_import_file)
            if status == 1:
                rootLogger.info(f"(12.{k}) [+] Import log saved")
            else:
                rootLogger.error(f"(12.{k}) >> Some error occurred during log saving ... ") 
        else:
            rootLogger.warning(" (12.{k}) >> Please check Import Job status later or repeat it...")

    rootLogger.info(f"\n=== Export and Import is finished | CI_CD_SESSION_ID: {CI_CD_SESSION_ID} ===")



    