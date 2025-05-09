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
EX_IC_USERNAME = os.getenv("DEV_IC_USERNAME")
EX_IC_PASSWORD = os.getenv("DEV_IC_PASSWORD")
EX_IC_LOGIN_URL = os.getenv("DEV_IC_LOGIN_URL")

#ENV for Import
env_path = Path(f'{module_path}/env/.env_qa')
load_dotenv(dotenv_path=env_path)
IM_IC_USERNAME = os.getenv("QA_IC_USERNAME")
IM_IC_PASSWORD = os.getenv("QA_IC_PASSWORD")
IM_IC_LOGIN_URL = os.getenv("QA_IC_LOGIN_URL")

#CI_CD SETTINGS
MODULE_NAME = "CDI"
print(f"MODULE_NAME: {MODULE_NAME}")
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
# os.makedirs(log_dir, exist_ok=True)

# Full path to the log file
# log_path = os.path.join(log_dir, log_file)

# logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-7.5s] %(message)s")
# rootLogger = logging.getLogger()
# rootLogger.setLevel(logging.DEBUG)

# fileHandler = logging.FileHandler(log_path)
# fileHandler.setFormatter(logFormatter)
# rootLogger.addHandler(fileHandler)

# consoleHandler = logging.StreamHandler()
# consoleHandler.setFormatter(logFormatter)
# rootLogger.addHandler(consoleHandler)

# rootLogger.info(f">> CI_CD_SESSION_ID: {CI_CD_SESSION_ID}")

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
        print("Authentication successful")
    else:
        print(f"status_code: {auth_response.status_code}")
        print(auth_response.text)
        raise Exception("Authentication denied!")
    return (auth_response.status_code, ic_server_url, ic_session_id)


def check_ic_object_valid_status(server_url: str, session_id: str, object_id: str):
    api_url = server_url + "/public/core/v3/export/" + export_id
    headers = {
        "INFA-SESSION-ID": session_id
    }
    response = requests.get(api_url, headers=headers)

    valid_status = ""
    if response.status_code == 200:
        data = json.loads(response.content)
        valid_status = data['status']["state"]
    else:
        print(f"Error {response.status_code}: {response.text}")
    return valid_status