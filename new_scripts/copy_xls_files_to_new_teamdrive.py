from DriveWrapper import DriveWrapper


def copy_xls_files_to_new_teamdrive(wrapper: DriveWrapper, dir_id_meetjaar_orig: str, dir_id_root_meetjaar: str):
    # create "Te_verwerken" en "Voor_wegendatabank" folders
    dir_ids = []
    for dir_name in ['Te_verwerken', 'Voor_wegendatabank']:
        dirs = wrapper.find_directory_by_name(directory_name=dir_name, directory_id=dir_id_root_meetjaar)
        if len(dirs) == 0:
            dir_ids.append(wrapper.create_directory(directory_name=dir_name, parent_id=dir_id_root_meetjaar))
        else:
            dir_ids.append(dirs[0])
    # if something fails: delete the "Te_verwerken" en "Voor_wegendatabank" folders and start over
    for file in wrapper.list_files_in_directory(dir_id_meetjaar_orig, recursive_level=1, file_types=['xls']):
        dirs = wrapper.find_directory_by_name(directory_name=file['parent_name'], directory_id=dir_ids[0])
        if len(dirs) == 0:
            parent_dir_id = wrapper.create_directory(directory_name=file['parent_name'], parent_id=dir_ids[0])
        else:
            parent_dir_id = dirs[0]

        # copy xls to "Te_verwerken" while keeping dir structure
        wrapper.copy_file_to_dir(file_id=file['id'], dir_id=parent_dir_id)
        # copy xls to "Voor_wegendatabank" ignoring dir structure
        wrapper.copy_file_to_dir(file_id=file['id'], dir_id=dir_ids[1])
    print("Done copying files to new Team Drive folder.")
