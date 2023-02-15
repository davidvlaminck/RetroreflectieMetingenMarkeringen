# pip install google-api-python-client

# "credentials_path": "C:\\resources\\driven-wonder-149715-ca8bdf010930.json"
from pathlib import Path

from DriveWrapper import DriveWrapper

if __name__ == '__main__':
    wrapper = DriveWrapper(
        service_cred_path=Path('/home/davidlinux/Documents/AWV/resources/driven-wonder-149715-ca8bdf010930.json'),
        readonly_scope=False)

    for file in wrapper.list_files_in_directory('1Me7ND5IpRTrVtXQjrfopjEsaVaxwtDqu', recursive_level=1, file_types=['xls']):
        print(file)
