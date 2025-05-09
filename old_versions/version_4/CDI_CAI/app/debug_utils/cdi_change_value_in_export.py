import json
import os
import time
import csv
import shutil
import zipfile
import pandas as pd
from pathlib import Path
import tempfile


current_path = Path(__file__)
module_path = f"{current_path.parent}"
module_path = module_path.replace("\\", "/")
print(f"module_path: {module_path}")

MODULE_NAME = 'CDI'
CI_CD_SESSION_ID = '1746457215504220'
CI_CD_DIRECTION = 'dev_to_qa'
PATH_MAP_CHANGE_VALUES = f"{module_path}/map_values"
PATH_EXPORT_FOLDER = f"{module_path}/export_to_import/{CI_CD_DIRECTION}/{MODULE_NAME}/{CI_CD_SESSION_ID}"



def recreate_export_module_original_folder(target_dir: str):
    if os.path.exists(target_dir):
        print(f"Folder '{target_dir}' already exists. Delete contents...")
        # Delete all files and folders inside
        for filename in os.listdir(target_dir):
            file_path = os.path.join(target_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                raise Exception(f"[X] Error while deleting {file_path}: {e}")
    else:
        print(f"[V] Folder '{target_dir}' not found. Creating directory...")
        os.makedirs(target_dir)
    return 1


def cdi_create_map_value_to_change(source_path: str, module_name: str, ci_cd_direction: str):
    file_path = None
    for fname in os.listdir(source_path):
        if fname.lower().endswith(('.xlsx', '.xls')):
            file_path = os.path.join(source_path, fname)
            break
    
    if not file_path:
        raise FileNotFoundError(f"File (.xlsx или .xls) not found in: {source_path}")

    print(f">> file map_change_values: {file_path}")

    # Read mapping with values for change in specific module
    sheet_name = module_name 
    try:
        df_map_change_values = pd.read_excel(file_path, sheet_name=sheet_name)
        print(df_map_change_values.head()) 
    except Exception as e:
        raise Exception(e)
    
    # Delete rows with any null-values
    df_cleaned_df_map_change_values = df_map_change_values.replace('', pd.NA).dropna()

    map_value_to_change = list()
    for index, row in df_cleaned_df_map_change_values.iterrows():
        if ci_cd_direction == 'dev_to_qa':
            value = {'old_value': row['Dev'], 'new_value': row['Test']}
        else:
            value ={'old_value': row['Test'], 'new_value': row['PROD']}
        map_value_to_change.append(value)

    return map_value_to_change



def cdi_change_export_json(path_export_folder: str, path_export_original: str, export_file: str, map_value_to_change: dict):
    dict_old_new_value = map_value_to_change.get(ic_object_path)
    print(f"dict_old_new_value: {dict_old_new_value}")
    
    path_export_file = f"{path_export_folder}/{export_file}"

    if not os.path.isfile(path_export_file):
        print(f"[X] No export file: {path_export_file}")
        return 0
    else:
        print("[V] Export file has found")

    # Working in a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        path_pattern_xml_file = f"Explore/{ic_object_path}"
        with zipfile.ZipFile(path_export_file, 'r') as archive:
            # Search for the first XML file by path pattern
            path_xml_file = None
            for f in archive.namelist():
                if f.startswith(path_pattern_xml_file) and f.endswith('.xml'):
                    path_xml_file = f
                    break

        if not path_xml_file:
            print(f"[X] XML-file not found: {path_pattern_xml_file}/*.xml")
            return 0
        else:
            print(f"[V] XML-file found: {path_xml_file}")

        # Preserve the original archive
        shutil.copy2(path_export_file, path_export_original)
        print(f"[V] Original archive saved to: {path_export_original}")

        # Unpack the archive into a temporary directory
        with zipfile.ZipFile(path_export_file, 'r') as archive:
            archive.extractall(temp_dir)

        xml_full_tmp_path = os.path.join(temp_dir, path_xml_file)
        xml_full_tmp_path = xml_full_tmp_path.replace("\\", "/")

        with open(xml_full_tmp_path, 'r', encoding='utf-8') as f:
            content = f.read() 

        print("---------------------------------------------------------------")
        print(f"old_new_value: {dict_old_new_value}")
        print("---------------------------------------------------------------")

        old_value = dict_old_new_value['old_value']
        new_value = dict_old_new_value['new_value']

        if old_value in content:
            print("[V] old_value found in XML")
            content = content.replace(old_value, new_value)
        else:
            print(f"[X] old_value '{old_value}' not found")
            return 0
            
        print("[V] XML content updated")
        #print(content)

        with open(xml_full_tmp_path, 'w') as f:
            f.write(content)
        print("[V] XML content saved")

        # Recreate ZIP archive with updated XML
        with zipfile.ZipFile(path_export_file, 'w') as updated_archive:
            for foldername, _, filenames in os.walk(temp_dir):
                for filename in filenames:
                    full_path = os.path.join(foldername, filename)
                    arcname = os.path.relpath(full_path, temp_dir)
                    updated_archive.write(full_path, arcname)
        print(f"[V] Archive updated and saved: {path_export_file}")
    return 1


# --- Entry point ---
if __name__ == "__main__":

    ###############################################################
    # Just to simulate list of objects
    list_cdi_objects = ['M_Call_SAP_6_W1', 'MT_Call_SAP_6_W1', 'MT_Call_SAP_6_W1']
    ################################################################

    path_export_module_original_folder = f"{PATH_EXPORT_FOLDER}/original"
    status = recreate_export_module_original_folder(path_export_module_original_folder)
    if status == 1:
        print(f"[V] Export_module_original_folder created: {path_export_module_original_folder}")
    else: 
        print(f"[X] Export_module_original_folder not created: {path_export_module_original_folder}")


    # create a collection to replace values ​​in export depending on the module
    map_value_to_change = None
    map_value_to_change = cdi_create_map_value_to_change(PATH_MAP_CHANGE_VALUES, MODULE_NAME, CI_CD_DIRECTION)

    print(f"\n map_value_to_change: {map_value_to_change}")

    file_name_for_change = 'exportMetadata.v2.json'
    # Need to change on original loop for export_import
    for obj in list_cdi_objects:
        ic_object_name = obj
        print("============================================================================")
        export_file = f"{ic_object_name}-{CI_CD_SESSION_ID}.zip"
        print(f"\n{export_file}")

        # status = cdi_change_export_json(PATH_EXPORT_FOLDER, path_export_module_original_folder, export_file, map_value_to_change)
        # if status == 1:
        #     print(f"[V] Value changes made to export file")
        # else:
        #     print(f"[-] Value changes skipped or not made")

            
