import time
import subprocess
import os
import logging
import requests
import json
import pandas as pd
import glob
from dotenv import load_dotenv
from pathlib import Path

##########################################################################################
# ========= 0. set up global variables and logging settings =========
print(" ========= 0. set up global variables and logging settings ========= ")
# Determain path to modules folder
current_path = Path(__file__)
current_folder_path = f"{current_path.parent.parent}"
current_folder_path = current_folder_path.replace("\\", "/")

#ENV for Export
env_path = Path(f'{current_folder_path}/env/.env_dev')
load_dotenv(dotenv_path=env_path)
EX_IC_USERNAME = os.getenv("DEV_IC_USERNAME")
EX_IC_PASSWORD = os.getenv("DEV_IC_PASSWORD")
EX_IC_LOGIN_URL = os.getenv("DEV_IC_LOGIN_URL")

#ENV for Import
env_path = Path(f'{current_folder_path}/env/.env_qa')
load_dotenv(dotenv_path=env_path)
IM_IC_USERNAME = os.getenv("QA_IC_USERNAME")
IM_IC_PASSWORD = os.getenv("QA_IC_PASSWORD")
IM_IC_LOGIN_URL = os.getenv("QA_IC_LOGIN_URL")

CI_CD_SESSION_ID = str(int( time.time_ns() / 1000 ))
print(f"CI_CD_SESSION_ID: {CI_CD_SESSION_ID}")

CI_CD_DIRECTION = "dev_to_qa"
MAIN_CI_CD_TASK_FOLDER = f"{current_folder_path}/ci_cd_task/{CI_CD_DIRECTION}"
IMPORT_CONFLICT_RESOLUTION = "OVERWRITE"
MAIN_ORCHESTARTOR_MODULE_NAME = "MAIN_ORCHESTRATOR"

MODULE_FOLDER = f"{current_folder_path}/module"
print(f"MODULE_FOLDER: {MODULE_FOLDER}")

LOG_MODULE_FOLDER = f"{current_folder_path}/log/module"
LOG_CI_CD_SESSION_FOLDER = f"{current_folder_path}/log/log_ci_cd_session"

##### set up logging #####
class SafeExtraFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'module_name'):
            record.module_name = ''  # значение по умолчанию, если extra не передали
        return super().format(record)

# Specify the directory and name of the log file
log_dir = LOG_CI_CD_SESSION_FOLDER
log_file = f"{CI_CD_SESSION_ID}.log"

# Create the directory if it does not exist
os.makedirs(log_dir, exist_ok=True)

# Full path to the log file
LOG_CI_CD_SESSION_FILE_PATH = os.path.join(log_dir, log_file)
print(f"LOG_CI_CD_SESSION_FILE_PATH: {LOG_CI_CD_SESSION_FILE_PATH}")

logFormatter = SafeExtraFormatter("%(module_name)s  %(asctime)s [%(threadName)-12.12s] [%(levelname)-7.5s] %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler(LOG_CI_CD_SESSION_FILE_PATH)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

str_module_name_value = f"[module: {MAIN_ORCHESTARTOR_MODULE_NAME}]"
extra_log_param = {'module_name': str_module_name_value}
adapterLogger = logging.LoggerAdapter(rootLogger, extra_log_param)


########################################################################################
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


def create_ci_cd_task_file(source_ci_cd_task_path: str, target_ci_cd_task_path: str, module_name: str, target_ci_cd_task_file: str):
    input_dir = source_ci_cd_task_path
    output_dir = target_ci_cd_task_path
    sheet_module = module_name 

    # Looking for Excel-file (.xls, .xlsx)
    file_pattern = os.path.join(input_dir, "*.xls*")
    matching_files = glob.glob(file_pattern)

    if not matching_files:
        raise FileNotFoundError(f"[X] No Excel files (.xls, .xlsx) in folder '{input_dir}'")

    input_file = matching_files[0]
    adapterLogger.info(f"[V] Main cid_cd file found: {input_file}")

    # Create folder for module ci_cd task 
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    else:
        # Clean module folder for ci_Cd task:
        for file_path in glob.glob(os.path.join(output_dir, "*")):
            os.remove(file_path)
        adapterLogger.info(f"Folder '{output_dir}' cleaned")

    # Read Sheet with objects list:
    try:
        df = pd.read_excel(input_file, sheet_name=sheet_module)
    except ValueError as e:
        raise ValueError(f"Sheet '{sheet_module}' not found if file '{input_file}'") from e

    # Create ci_cd task file for module:
    output_file = os.path.join(output_dir, target_ci_cd_task_file)
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    adapterLogger.info(f"Sheet '{sheet_module}' saved in: {output_file}")
    return 1
########################################################################################


########################################################################################
# --- Entry point ---
if __name__ == "__main__":

    adapterLogger.info(f"\n========= START | CI_CD_SESSION_ID: {CI_CD_SESSION_ID} | CI_CD_DIRECTION: {CI_CD_DIRECTION} ========= ")

    # ========= Authorization =========
    adapterLogger.info("\n========= Authorization in Export env ========= ")
    auth_response_code, ex_ic_server_url, ex_ic_session_id = ic_authentication(EX_IC_LOGIN_URL, EX_IC_USERNAME, EX_IC_PASSWORD)
    adapterLogger.info(f"ex_ic_server_url: {ex_ic_server_url}")
    adapterLogger.info(f"ex_ic_session_id: {ex_ic_session_id}")

    adapterLogger.info("\n========= Authorization in Import env ========= ")
    auth_response_code, im_ic_server_url, im_ic_session_id = ic_authentication(IM_IC_LOGIN_URL, IM_IC_USERNAME, IM_IC_PASSWORD)
    adapterLogger.info(f"im_ic_server_url: {im_ic_server_url}")
    adapterLogger.info(f"im_ic_session_id: {im_ic_session_id}")

    # ========= Prepare CI_CD mappings =========
    adapterLogger.info(f"\n========= Prepare CI_CD mappings ========= ")
    params_collection = {
        "ex_ic_server_url": ex_ic_server_url,
        "ex_ic_session_id": ex_ic_session_id,
        "im_ic_server_url": im_ic_server_url,
        "im_ic_session_id": im_ic_session_id,
        "module_folder": MODULE_FOLDER,
        "module_name": "",
        "ci_cd_session_id": CI_CD_SESSION_ID,
        "ci_cd_direction": CI_CD_DIRECTION,
        "import_conflict_resolution": IMPORT_CONFLICT_RESOLUTION,
        "log_module_foler": LOG_MODULE_FOLDER,
        "log_ci_cd_session_folder": LOG_CI_CD_SESSION_FOLDER,
        "log_ci_cd_session_file_path": LOG_CI_CD_SESSION_FILE_PATH
    }

    map_module_path = {
        "R360": f"{MODULE_FOLDER}/R360/app/main.py",
        "CDI": f"{MODULE_FOLDER}/CDI_CAI/app/main.py",
        "CAI": f"{MODULE_FOLDER}/CDI_CAI/app/main.py",
        "CDQ": f"{MODULE_FOLDER}/CDQ/app/main.py"
    }

    # Excel tabs names in correct order
    ci_cd_module_order = ['R360', 'CDI', 'CAI', 'CDQ']

    # ========= Start CI_CD process =========
    adapterLogger.info(f"\n ========= Start CI_CD process ========= ")
    for module_name in ci_cd_module_order:
        adapterLogger.info(f"\n ========= Create ci_cd task file for module: {module_name} ========= ")
        if module_name in ('CDI', 'CAI'):
            module_name_path = 'CDI_CAI'
        else:
            module_name_path = module_name
        module_ci_cd_task_path = f"{MODULE_FOLDER}/{module_name_path}/ci_cd_task/{CI_CD_DIRECTION}"
        module_ci_cd_task_file_name = f"{module_name}-{CI_CD_SESSION_ID}.csv"
        try:
            result = create_ci_cd_task_file(MAIN_CI_CD_TASK_FOLDER, module_ci_cd_task_path, module_name, module_ci_cd_task_file_name)
        except Exception as e:
            adapterLogger.error(e)
            raise Exception(e)
        
        module_path = map_module_path.get(module_name)

        params_collection["module_name"] = module_name
        str_params_collection = json.dumps(params_collection)

        adapterLogger.info(f"\n ========= Start run module: {module_name} ========= ")
        adapterLogger.info(f"\n str_params_collection for [{module_name}]: {str_params_collection} ")
        try:
            subprocess.run(["python", module_path, str_params_collection])
        except Exception as e:
            adapterLogger.error(e)
            raise Exception(e)
        adapterLogger.info(f"\n ========= Finish run module: {module_name} ========= ")

    adapterLogger.info(f"\n ========= FINISH | CI_CD_SESSION_ID: {CI_CD_SESSION_ID} | CI_CD_DIRECTION: {CI_CD_DIRECTION} ========= ")


