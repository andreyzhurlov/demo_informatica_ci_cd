### PROJECT ARCHITECTURE ###

#Export to Import task - list of objects:
|-ci_cd_task
|--dev_to_qa
|---<list_objects>.csv
    (only one file or the first one from the list will be taken and upon completion of the process - moved to the "log_ci_cd_task" folder)


#System variables:
|-env
|--.env


#File storage for export and subsequent import
|-export_to_import
|--dev_to_qa
|---<timestamp>
|----<object_name>-<timestamp>.zip


#Logging:
|-log
|--log_ci_cd_session
|---<timestamp>.txt
    (briefly about all the steps of export and import within one session based on the list of objects from one CSV file)
|--log_ci_cd_task
|---<ci_cd_sesstion_id = timestamp>
|----<list_objects>.csv
|--log_export
|---<timestamp>
    (folder "timestamp - this is "session_id", in which for each object from the list for export, a separate file with detailed logs is stored)
|----ex_<object_name>-<timestamp>.txt
|--log_import
|---<timestamp>
    (folder in which for each object from the list for export, a separate file with detailed logs is stored)
|----im_<object_name>-<timestamp>.txt

#Virtual environment:
|-venv

#Previous version of the project:
|-version_1

###############################