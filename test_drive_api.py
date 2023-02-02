# pip install google-api-python-client

# "credentials_path": "C:\\resources\\driven-wonder-149715-ca8bdf010930.json"
from pathlib import Path

from DriveWrapper import DriveWrapper

if __name__ == '__main__':
    wrapper = DriveWrapper(
        service_cred_path=Path('/home/davidlinux/Documents/AWV/resources/driven-wonder-149715-ca8bdf010930.json'),
        readonly_scope=False)

    print(wrapper.list_files_in_directory('1cId78b0m3rytq2P05cLh52eJ3wk7z7h1', recursive=True))
