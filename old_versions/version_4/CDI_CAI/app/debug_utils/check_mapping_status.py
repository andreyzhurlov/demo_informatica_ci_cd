import requests
import json
import sys
import os
import time
import csv
import logging
from dotenv import load_dotenv
from pathlib import Path


# ##########################################################################################
# # === 0. set up global variables and logging settings ===
# path to current file
current_path = Path(__file__)
module_path = f"{current_path.parent.parent.parent}"
module_path = module_path.replace("\\", "/")
print(f"module_path: {module_path}")

#ENV for Export
# env_path = Path(f'{module_path}/env/.env_dev')
# load_dotenv(dotenv_path=env_path)
# EX_IC_USERNAME = os.getenv("DEV_IC_USERNAME")
# EX_IC_PASSWORD = os.getenv("DEV_IC_PASSWORD")
# EX_IC_LOGIN_URL = os.getenv("DEV_IC_LOGIN_URL")

#ENV for Import
env_path = Path(f'{module_path}/env/.env_qa')
load_dotenv(dotenv_path=env_path)
IM_IC_USERNAME = os.getenv("QA_IC_USERNAME")
IM_IC_PASSWORD = os.getenv("QA_IC_PASSWORD")
IM_IC_LOGIN_URL = os.getenv("QA_IC_LOGIN_URL")



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



def get_all_mappings_api_v2(server_url: str, session_id: str):
    api_url = server_url + f"/api/v2/mapping/"
    headers = {
        "icSessionId": session_id
    }
    response = requests.get(api_url, headers=headers)

    list_of_objects = dict()
    if response.status_code == 200:
        list_of_objects = json.loads(response.content)
        return list_of_objects
    else:
        print(f"Error {response.status_code}: {response.text}")
        return list_of_objects


def is_mapping_valid(server_url: str, session_id: str, ic_object_id: str):
    # get all mappings
    list_of_objects = get_all_mappings_api_v2(server_url, session_id)

    # Search mapping by value in assetFrsGuid
    match = next((item for item in list_of_objects if item.get("assetFrsGuid") == ic_object_id), None)
    
    # Check - is mapping valid
    is_valid = False
    if match:
        print(f"[V] Mapping found with assetFrsGuid = {ic_object_id} | mappingTemplate_id = {match['id']}")
        is_valid = match['valid']
        print(f"is_valid: {is_valid}")
    else:
        print(f"[X] Mapping  with assetFrsGuid = {ic_object_id} NOT found")
    return is_valid 



########################################################################################
# --- Entry point ---
if __name__ == "__main__":
    print("\n=== 1.2 Authorization in Import env ===")
    auth_response_code, im_ic_server_url, im_ic_session_id = ic_authentication(IM_IC_LOGIN_URL, IM_IC_USERNAME, IM_IC_PASSWORD)
    print(f"im_ic_server_url: {im_ic_server_url}")
    print(f"im_ic_session_id: {im_ic_session_id}")

    ic_object_type = 'Mapping'

    # Check object type
    if ic_object_type.lower() == 'mapping':
        print("\n=== Check Mapping status ===")
        # Example -  [True, False, True]
        list_of_mapping_id = ['bWbbseCDNMlkTu7gGYGOoT','btttbseCqqqqqqaGYGOo', '8PQLN3bB82EfLMhnVaAoil']
        for ic_object_id in list_of_mapping_id:
            print("\n-------------------------------------")
            is_valid = is_mapping_valid(im_ic_server_url, im_ic_session_id, ic_object_id)
            print(f">> {ic_object_id} | is_valid: {is_valid}")

#ic_object_id = "bWbbseCDNMlkTu7gGYGOoT"

# Example of API response
#     str_json_data = """
# [
#     {
#         "@type": "mappingTemplate",
#         "id": "011BPE1700000000008K",
#         "orgId": "011BPE",
#         "name": "B2B_Gateway_EDI_Inbound_Mapping",
#         "description": "2024_04_M/29.02",
#         "createTime": "2024-04-14T09:41:33.000Z",
#         "updateTime": "2024-04-14T09:41:33.000Z",
#         "createdBy": "bundle-license-notifier",
#         "updatedBy": "bundle-license-notifier",
#         "autoExpireObject": false,
#         "bundleVersion": "0",
#         "assetFrsGuid": "6ZM9Ym0i9mmbhuJflc2U0n",
#         "templateId": "stringIdentity:011BPE0X0000000001M4",
#         "deployTime": 1713102093000,
#         "hasParameters": true,
#         "valid": true,
#         "fixedConnection": false,
#         "hasParametersDeployed": true,
#         "fixedConnectionDeployed": false,
#         "isSchemaValidationEnabled": false,
#         "deployedTemplateId": "stringIdentity:011BPE0X0000000001M5",
#         "tasks": 0,
#         "mappingPreviewFileRecordId": "011BPE0X0000000001M3",
#         "deployedMappingPreviewFileRecordId": "011BPE0X0000000001M6",
#         "documentType": "",
#         "allowMaxFieldLength": false
#     },
#     {
#         "@type": "mappingTemplate",
#         "id": "011BPE17000000000029",
#         "orgId": "011BPE",
#         "name": "B2B_Gateway_EDI_Inbound_Mapping",
#         "description": "",
#         "createTime": "2023-05-08T13:13:30.000Z",
#         "updateTime": "2023-05-08T13:13:30.000Z",
#         "createdBy": "bundle-license-notifier",
#         "updatedBy": "bundle-license-notifier",
#         "autoExpireObject": false,
#         "bundleVersion": "0",
#         "assetFrsGuid": "7q1ZFCqrZkXhvL8YrSqJq3",
#         "templateId": "stringIdentity:011BPE0X00000000008P",
#         "deployTime": 1683566011000,
#         "hasParameters": true,
#         "valid": true,
#         "fixedConnection": false,
#         "hasParametersDeployed": true,
#         "fixedConnectionDeployed": false,
#         "isSchemaValidationEnabled": false,
#         "deployedTemplateId": "stringIdentity:011BPE0X000000000090",
#         "tasks": 0,
#         "mappingPreviewFileRecordId": "011BPE0X00000000008N",
#         "deployedMappingPreviewFileRecordId": "011BPE0X000000000091",
#         "documentType": "",
#         "allowMaxFieldLength": false
#     },
#     {
#         "@type": "mappingTemplate",
#         "id": "011BPE1700000000002A",
#         "orgId": "011BPE",
#         "name": "B2B_Gateway_EDI_Inbound_Mapping",
#         "description": "",
#         "createTime": "2023-05-08T13:13:30.000Z",
#         "updateTime": "2023-05-08T13:13:30.000Z",
#         "createdBy": "bundle-license-notifier",
#         "updatedBy": "bundle-license-notifier",
#         "autoExpireObject": false,
#         "bundleVersion": "0",
#         "assetFrsGuid": "bWbbseCDNMlkTu7gGYGOoT",
#         "templateId": "stringIdentity:011BPE0X00000000008V",
#         "deployTime": 1683566011000,
#         "hasParameters": true,
#         "valid": true,
#         "fixedConnection": false,
#         "hasParametersDeployed": true,
#         "fixedConnectionDeployed": false,
#         "isSchemaValidationEnabled": false,
#         "deployedTemplateId": "stringIdentity:011BPE0X000000000097",
#         "tasks": 0,
#         "mappingPreviewFileRecordId": "011BPE0X00000000008U",
#         "deployedMappingPreviewFileRecordId": "011BPE0X000000000099",
#         "documentType": "",
#         "allowMaxFieldLength": false
#     },
#     {
#         "@type": "mappingTemplate",
#         "id": "011BPE1700000000002C",
#         "orgId": "011BPE",
#         "name": "B2B_Gateway_EDI_Inbound_Mapping",
#         "description": "2021_04_S",
#         "createTime": "2023-05-08T13:13:31.000Z",
#         "updateTime": "2023-05-08T13:13:31.000Z",
#         "createdBy": "bundle-license-notifier",
#         "updatedBy": "bundle-license-notifier",
#         "autoExpireObject": false,
#         "bundleVersion": "0",
#         "assetFrsGuid": "6dV7XOpSdeZkRom9QbMw2R",
#         "templateId": "stringIdentity:011BPE0X000000000093",
#         "deployTime": 1683566011000,
#         "hasParameters": true,
#         "valid": true,
#         "fixedConnection": false,
#         "hasParametersDeployed": true,
#         "fixedConnectionDeployed": false,
#         "isSchemaValidationEnabled": false,
#         "deployedTemplateId": "stringIdentity:011BPE0X00000000009E",
#         "tasks": 0,
#         "mappingPreviewFileRecordId": "011BPE0X000000000092",
#         "deployedMappingPreviewFileRecordId": "011BPE0X00000000009F",
#         "documentType": "",
#         "allowMaxFieldLength": false
#     }
# ]
#     """
