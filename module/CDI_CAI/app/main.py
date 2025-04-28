import requests
import json
import sys
import os
import time
import csv
import logging

from pathlib import Path


##########################################################################################
# === 0. set up global variables and logging settings ===
print(" === 0. set up global variables and logging settings === ")
str_params_collection = sys.argv[1]
params_collection = json.loads(str_params_collection)

MODULE_NAME = params_collection.get('module_name')
MODULE_FOLDER = f"{params_collection.get('module_folder')}/CDI_CAI" # <<<

EX_IC_SERVER_URL = params_collection.get('ex_ic_server_url')
EX_IC_SESSION_ID = params_collection.get('ex_ic_session_id')

IM_IC_SERVER_URL = params_collection.get('im_ic_server_url')
IM_IC_SESSION_ID = params_collection.get('im_ic_session_id')

CI_CD_SESSION_ID = params_collection.get('ci_cd_session_id')
CI_CD_DIRECTION = params_collection.get('ci_cd_direction')
CI_CD_TASK_PATH = f"{MODULE_FOLDER}/ci_cd_task/{CI_CD_DIRECTION}"

LOG_MODULE_FOLDER = params_collection.get('log_module_foler')
LOG_CI_CD_SESSION_FOLDER = params_collection.get('log_module_foler')
LOG_CI_CD_SESSION_FILE_PATH = params_collection.get('log_ci_cd_session_file_path')

LOG_CI_CD_TASK = f"{LOG_MODULE_FOLDER}/{MODULE_NAME}/log_ci_cd_task"

EXPORT_FOLDER = f"{MODULE_FOLDER}/export_to_import/{CI_CD_DIRECTION}"
LOG_EXPORT_FOLDER = f"{LOG_MODULE_FOLDER}/{MODULE_NAME}/log_export"

IMPORT_CONFLICT_RESOLUTION = params_collection.get('import_conflict_resolution')
LOG_IMPORT_FOLDER = f"{LOG_MODULE_FOLDER}/{MODULE_NAME}/log_import"

##### set up logging #####
class SafeExtraFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'module_name'):
            record.module_name = ''
        return super().format(record)
    
logFormatter = SafeExtraFormatter("%(module_name)s  %(asctime)s [%(threadName)-12.12s] [%(levelname)-7.5s] %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler(LOG_CI_CD_SESSION_FILE_PATH)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

str_module_name_value = f"[module: {MODULE_NAME}]"
extra_log_param = {'module_name': str_module_name_value}
adapterLogger = logging.LoggerAdapter(rootLogger, extra_log_param)
##########################################################################################



# EXPORT UTILS ###########################################################################

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


def create_object_collection(source_object_list: str):
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


def load_export_package(server_url: str, session_id: str, export_id: str, export_to_import_path: str):
    if not os.path.exists(export_folder):
        os.makedirs(export_folder)
        rootLogger.info(f">> Export directory created: {export_folder}")

    api_url = server_url + "/public/core/v3/export/" + export_id + "/package"
    headers = {
        "INFA-SESSION-ID": session_id
    }
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        with open(export_to_import_path, "wb") as f:
            f.write(response.content)
        rootLogger.info(f"[V] Package saved successfully in path '{export_to_import_path}'")
        return 1
    else:
        rootLogger.error(f"[X] Error: status {response.status_code}")
        rootLogger.error(response.text)
        return 0


def load_export_log(server_url: str, session_id: str, export_id: str, log_folder: str, log_file: str):
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
        rootLogger.info(f">> Export Log directory created: {log_folder}")
    log_path = f"{log_folder}/{log_file}"

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
    log_path = f"{log_folder}/{log_file}"

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
    adapterLogger.info(f"\n================================================================================= ")
    adapterLogger.info(f"\n=== Start | CI_CD_SESSION_ID: {CI_CD_SESSION_ID} | CI_CD_DIRECTION: {CI_CD_DIRECTION} ==== ")

    # === 1. Check params ===
    adapterLogger.info("\n=== 1. Check params === ")
    adapterLogger.info(f"MODULE_NAME: {MODULE_NAME} | CI_CD_SESSION_ID: {CI_CD_SESSION_ID} | CI_CD_DIRECTION: {CI_CD_DIRECTION} | \n \
                    EX_IC_SERVER_URL: {EX_IC_SERVER_URL} | EX_IC_SESSION_ID: {EX_IC_SESSION_ID} | \n \
                    IM_IC_SERVER_URL: {IM_IC_SERVER_URL} | IM_IC_SESSION_ID: {IM_IC_SESSION_ID} | \n \
                    MODULE_FOLDER: {MODULE_FOLDER} | IMPORT_CONFLICT_RESOLUTION: {IMPORT_CONFLICT_RESOLUTION} \n \
                    LOG_MODULE_FOLDER: {LOG_MODULE_FOLDER} | LOG_CI_CD_SESSION_FILE_PATH: {LOG_CI_CD_SESSION_FILE_PATH}")

    # === 2. Get list of objects to export ===
    adapterLogger.info("\n=== 2. Get list of objects to export ===")
    list_object_to_export = get_object_list_to_export(CI_CD_TASK_PATH)

    # === 7. Create cdi_objects collection ===
    adapterLogger.info("\n=== 3. Create cdi_objects collection ===")
    cdi_cai_object_collection = dict()

    adapterLogger.info("(3) add Mappings")
    mapping_list = get_all_objects_by_type(EX_IC_SERVER_URL, EX_IC_SESSION_ID, 'Mapping')
    mapping_collection = create_object_collection(mapping_list)
    cdi_cai_object_collection.update(mapping_collection)

    adapterLogger.info("(3) add Mapping Tasks")
    mt_list = get_all_objects_by_type(EX_IC_SERVER_URL, EX_IC_SESSION_ID, 'MTT')
    mt_collection = create_object_collection(mt_list)
    cdi_cai_object_collection.update(mt_collection)

    adapterLogger.info("(3) add TaskFlows")
    tf_list = get_all_objects_by_type(EX_IC_SERVER_URL, EX_IC_SESSION_ID, 'TASKFLOW')
    tf_collection = create_object_collection(tf_list)
    cdi_cai_object_collection.update(tf_collection)

    adapterLogger.info("(3) add ServiceConnectors")
    sc_list = get_all_objects_by_type(EX_IC_SERVER_URL, EX_IC_SESSION_ID, 'AI_SERVICE_CONNECTOR')
    sc_collection = create_object_collection(sc_list)
    cdi_cai_object_collection.update(sc_collection)

    adapterLogger.info("(3) add Processes")
    pr_list = get_all_objects_by_type(EX_IC_SERVER_URL, EX_IC_SESSION_ID, 'PROCESS')
    pr_collection = create_object_collection(pr_list)
    cdi_cai_object_collection.update(pr_collection)

    adapterLogger.info("(3) add AppConnectors")
    ac_list = get_all_objects_by_type(EX_IC_SERVER_URL, EX_IC_SESSION_ID, 'AI_CONNECTION')
    ac_collection = create_object_collection(ac_list)
    cdi_cai_object_collection.update(ac_collection)


    adapterLogger.info("\n=== 4. Search id for objects ====")
    map_object_to_export = dict()
    for row in list_object_to_export:
        obj_path = f"{row[2]}/{row[3]}"
        obj_id = cdi_cai_object_collection.get(obj_path, 'not found')
        map_object_to_export[obj_path] = (row[3], obj_id)
    adapterLogger.info(f"(4) map_object_to_export: \n{map_object_to_export}")

    for obj_name, obj_id in map_object_to_export.values():
        if obj_id == 'not found':
            adapterLogger.error("(4) For some objects the ID was not found, check the logs for the full list")
            raise Exception("(4) [Error]: For some objects the ID was not found, check the logs for the full list")

    # === 5. Run Export Job for each object ===
    adapterLogger.info("\n=== 5. Run Export - Import Job for each object ===")
    export_session_folder = f"{EXPORT_FOLDER}/{CI_CD_SESSION_ID}"
    log_export_folder_session = f"{LOG_EXPORT_FOLDER}/{CI_CD_SESSION_ID}"
    log_import_folder_session = f"{LOG_IMPORT_FOLDER}/{CI_CD_SESSION_ID}"

    # Exporting
    k = 0
    for ic_object_path, ic_object_metadata in map_object_to_export.items():
        k += 1
        ic_object_name = ic_object_metadata[0]
        ic_object_id = ic_object_metadata[1]
        adapterLogger.info(f"\n(5.{k}) >> ic_object_path: {ic_object_path} | ic_object_name: {ic_object_name} | ic_object_id: {ic_object_id}")

        export_job_name = f"{ic_object_name}-{CI_CD_SESSION_ID}"
        adapterLogger.info(f"\n(5.{k}) >> export_job_name: {export_job_name}")
        ic_export_job_id = create_export_job(EX_IC_SERVER_URL, EX_IC_SESSION_ID, export_job_name, ic_object_id)
        adapterLogger.info(f"(5.{k}) >> ic_export_job_id: {ic_export_job_id}")
        time.sleep(3)

        # === 6. Checking Export Job status ===
        adapterLogger.info(f"\n===  6.{k} Checking Export Job status ===")
        # X checks with pause in N sec
        n_attempts = 11
        pause_sec = 3
        ic_export_job_status = ""
        for i in range(1, n_attempts):
            ic_export_job_status = check_export_job_status(EX_IC_SERVER_URL, EX_IC_SESSION_ID, ic_export_job_id)
            adapterLogger.info(f"(6.{k}) >> [{i}] check ic_export_job_status: {ic_export_job_status}")
            if ic_export_job_status == "SUCCESSFUL":
                break
            time.sleep(pause_sec)

        export_file = f"{ic_object_name}-{CI_CD_SESSION_ID}.zip"
        export_folder = export_session_folder
        export_to_import_path = f"{export_folder}/{export_file}"
        if ic_export_job_status == "SUCCESSFUL":
            # === 7. Load Export Package ===
            adapterLogger.info(f"\n===  7.{k} Load Export Package ===")
            status = load_export_package(EX_IC_SERVER_URL, EX_IC_SESSION_ID, ic_export_job_id, export_to_import_path)
            if status == 1:
                adapterLogger.info(f"(7.{k}) -=[~+~] Package exported successfully [~+~]=-")
            else:
                adapterLogger.error(f"(7.{k}) >> Some error occurred during export... ")  
        else:
            adapterLogger.warning(" (7.{k}) >> Please check Export Job status later or repeat it...")

        # === 8. Load Export Package ===
        adapterLogger.info(f"\n===  8.{k} Load Export Package Log ===")
        log_export_file = f"ex_{ic_object_name}-{CI_CD_SESSION_ID}.txt"
        status = load_export_log(EX_IC_SERVER_URL, EX_IC_SESSION_ID, ic_export_job_id, log_export_folder_session, log_export_file)
        if status == 1:
            adapterLogger.info(f"(8.{k}) [+] Export log saved")
        else:
            adapterLogger.error(f"(8.{k}) >> Some error occurred during log saving ... ")

        adapterLogger.info("==========================================================")

        # === 9. Upload Import Package === 
        adapterLogger.info("\n=== 9. Upload Import Package === ")
        ic_import_job_id = upload_import_package(IM_IC_SERVER_URL, IM_IC_SESSION_ID, export_to_import_path)
        adapterLogger.info(f"(9.{k}) ic_import_job_id: {ic_import_job_id}")
        if ic_import_job_id == 0:
            raise Exception(f"(9.{k}) [Error]: ic_import_job_id is invalid, please check logs")

        # === 10. Create Import Job ===
        adapterLogger.info("\n=== 10. Create Import Job ===")
        import_job_name = export_job_name
        list_object_id = [ic_object_id]

        ic_import_job_status = create_import_job(IM_IC_SERVER_URL, IM_IC_SESSION_ID, ic_import_job_id, import_job_name, list_object_id, IMPORT_CONFLICT_RESOLUTION)
        adapterLogger.info(f"(10) ic_import_job_status: {ic_import_job_status}")
        time.sleep(3)
        
        # === 11. Checking Import Job status ===
        adapterLogger.info(f"\n=== 11.{k} Checking Import Job status ===")
        # X checks with pause in N sec
        n_attempts = 15
        pause_sec = 3
        ic_import_job_status = ""
        for i in range(1, n_attempts):
            ic_import_job_status = check_export_job_status(IM_IC_SERVER_URL, IM_IC_SESSION_ID, ic_export_job_id)
            adapterLogger.info(f"(11.{k}) >> [{i}] check ic_import_job_status: {ic_import_job_status}")
            if ic_import_job_status == "SUCCESSFUL":
                break
            time.sleep(pause_sec)
        
        if ic_import_job_status == "SUCCESSFUL":
            # === 12. Load Import  Log  ===
            adapterLogger.info(f"\n=== 12.{k} Load Import  Log ===")
            log_import_file = f"im_{ic_object_name}-{CI_CD_SESSION_ID}.txt"
            status = load_import_log(IM_IC_SERVER_URL, IM_IC_SESSION_ID, ic_import_job_id, log_import_folder_session, log_import_file)
            if status == 1:
                adapterLogger.info(f"(12.{k}) [+] Import log saved")
            else:
                adapterLogger.error(f"(12.{k}) >> Some error occurred during log saving ... ") 
        else:
            adapterLogger.warning(" (12.{k}) >> Please check Import Job status later or repeat it...")
        
    adapterLogger.info(f"\n=== Export and Import is finished | CI_CD_SESSION_ID: {CI_CD_SESSION_ID} ===")
