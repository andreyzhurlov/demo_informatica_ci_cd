import os
import sys
import json
import logging


##########################################################################################
# === 0. set up global variables and logging settings ===
print(" === 0. set up global variables and logging settings === ")
str_params_collection = sys.argv[1]
params_collection = json.loads(str_params_collection)

MODULE_NAME = params_collection.get('module_name')
MODULE_FOLDER = f"{params_collection.get('module_folder')}/R360" # <<<

EX_IC_SERVER_URL = params_collection.get('ex_ic_server_url')
EX_IC_SESSION_ID = params_collection.get('ex_ic_session_id')

IM_IC_SERVER_URL = params_collection.get('im_ic_server_url')
IM_IC_SESSION_ID = params_collection.get('im_ic_session_id')

CI_CD_SESSION_ID = params_collection.get('ci_cd_session_id')
CI_CD_DIRECTION = params_collection.get('ci_cd_direction')
IMPORT_CONFLICT_RESOLUTION = params_collection.get('import_conflict_resolution')

LOG_MODULE_FOLDER = params_collection.get('log_module_foler')
LOG_CI_CD_SESSION_FOLDER = params_collection.get('log_module_foler')
LOG_CI_CD_SESSION_FILE_PATH = params_collection.get('log_ci_cd_session_file_path')

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


########################################################################################
# --- Entry point ---
if __name__ == "__main__":
    adapterLogger.info(f"\n================================================================================= ")
    adapterLogger.info(f"\n=== Start | CI_CD_SESSION_ID: {CI_CD_SESSION_ID} | CI_CD_DIRECTION: {CI_CD_DIRECTION} ==== ")


    adapterLogger.info(f"MODULE_NAME: {MODULE_NAME} | CI_CD_SESSION_ID: {CI_CD_SESSION_ID} | CI_CD_DIRECTION: {CI_CD_DIRECTION} | \n \
                    EX_IC_SERVER_URL: {EX_IC_SERVER_URL} | EX_IC_SESSION_ID: {EX_IC_SESSION_ID} | \n \
                    IM_IC_SERVER_URL: {IM_IC_SERVER_URL} | IM_IC_SESSION_ID: {IM_IC_SESSION_ID} | \n \
                    MODULE_FOLDER: {MODULE_FOLDER} | IMPORT_CONFLICT_RESOLUTION: {IMPORT_CONFLICT_RESOLUTION} \n \
                    LOG_MODULE_FOLDER: {LOG_MODULE_FOLDER} | LOG_CI_CD_SESSION_FILE_PATH: {LOG_CI_CD_SESSION_FILE_PATH}")


    adapterLogger.info(f"\n=== Finish | CI_CD_SESSION_ID: {CI_CD_SESSION_ID} | CI_CD_DIRECTION: {CI_CD_DIRECTION} ==== ")
    adapterLogger.info(f"\n================================================================================= ")