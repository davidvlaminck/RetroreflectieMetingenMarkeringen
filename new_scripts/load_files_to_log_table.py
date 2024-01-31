import datetime

from DriveWrapper import DriveWrapper
from PostGISConnector import PostGISConnector


def load_files_to_log_table(connector: PostGISConnector, wrapper: DriveWrapper, root_meetjaar, report_year: int):
    dir_name = 'Te_verwerken'
    dir_id = wrapper.find_directory_by_name(directory_name=dir_name, directory_id=root_meetjaar)[0]

    insert_timestamp = datetime.datetime.utcnow()
    for file in wrapper.list_files_in_directory(dir_id, recursive_level=1, file_types=['xls']):
        insert_query = f"""
        WITH t (drive_file_name, drive_file_id, drive_folder_name, drive_folder_id, datetime_utc_read, completed_step, report_year) 
            AS (VALUES ('{file['name']}', '{file['id']}', '{file['parent_name']}', '{file['parent_id']}', 
                '{insert_timestamp}'::timestamp, 3, {report_year})),
        to_insert AS (
            SELECT t.* 
            FROM t
                LEFT JOIN ttw.log_files_state AS log_files_state ON 
                    log_files_state.drive_file_name = t.drive_file_name AND log_files_state.report_year = t.report_year
            WHERE log_files_state.drive_file_id IS NULL)
        INSERT INTO ttw.log_files_state (drive_file_name, drive_file_id, drive_folder_name, drive_folder_id, 
            datetime_utc_read, completed_step, report_year)
        SELECT to_insert.drive_file_name, to_insert.drive_file_id, to_insert.drive_folder_name, 
            to_insert.drive_folder_id, to_insert.datetime_utc_read, to_insert.completed_step, to_insert.report_year
        FROM to_insert;"""
        print(file)
        connector.execute_query(insert_query)
