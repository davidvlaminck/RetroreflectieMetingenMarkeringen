import json
import socket
from pathlib import Path
from typing import Dict

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError


class DriveWrapper:
    def __init__(self, service_cred_path: Path = None, readonly_scope: None | bool = None):
        if service_cred_path is None:
            raise NotImplementedError('only access with service account is supported')
        self.service_cred_path = service_cred_path
        self.credentials: None | Credentials = None

        socket.setdefaulttimeout(180)

        if readonly_scope is None:
            raise ValueError('set readonly_scope to True or False')

    def authenticate(self) -> Credentials:
        if self.credentials is not None:
            return self.credentials

        cred_file_path = self.service_cred_path
        if not cred_file_path.is_file():
            raise FileNotFoundError(f"could not find the credentials file at {cred_file_path}")

        with open(cred_file_path) as cred_file:
            gcp_sa_credentials = json.load(cred_file)

        self.credentials = service_account.Credentials.from_service_account_info(gcp_sa_credentials)
        return self.credentials

    def list_files_in_directory(self, directory_id: str, recursive: bool = False, file_types: [str] = None) -> [Dict]:
        return self.list_objects_in_directory(directory_id, return_directories=False,
                                              recursive=recursive, file_types=file_types)

    def list_directories_in_directory(self, directory_id: str, recursive: bool = False,
                                      file_types: [str] = None) -> [Dict]:
        return self.list_objects_in_directory(directory_id, return_files=False, recursive=recursive,
                                              file_types=file_types)

    def list_objects_in_directory(self, directory_id: str, return_files: bool = True, file_types: [str] = None,
                                  return_directories: bool = True, recursive: bool = False) -> [Dict]:
        """
        Lists objects in Google Drive folder given the id of the folder

        :param directory_id: the id of the Google Drive (see url after 'folders/')
        :param return_files: if False this will exclude files, defaults to True
        :param return_directories: if False this will exclude directories, defaults to True
        :param file_types: if not empty, this list will be used to filter the files by extension
        :param recursive: if True this will recursively go through directories in the given directory, defaults to False
        :return: Returns a list of dictionaries, each with the id, name and mimeType of the object
        """
        creds = self.authenticate()

        try:
            service = build('drive', 'v3', credentials=creds)
            files = []
            page_token = None
            while True:
                response = service.files().list(q=f"'{directory_id}' in parents",
                                                fields='nextPageToken, '
                                                       'files(id, name, mimeType)',
                                                includeItemsFromAllDrives=True,
                                                supportsAllDrives=True,
                                                pageToken=page_token).execute()
                for drive_object in response.get('files', []):
                    mimetype = drive_object.get("mimeType")
                    if mimetype == 'application/vnd.google-apps.folder':
                        if return_directories:
                            files.append(drive_object)
                        if recursive:
                            files.extend(self.list_objects_in_directory(
                                directory_id=drive_object.get('id'), return_files=return_files, file_types=file_types,
                                return_directories=return_directories, recursive=recursive))
                    else:
                        if return_files:
                            if file_types:
                                extension = drive_object.get('name').split('.')[-1]
                                if extension in file_types:
                                    files.append(drive_object)
                            else:
                                files.append(drive_object)
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break

        except HttpError as error:
            print(F'An error occurred: {error}')
            files = None

        print(f'found files in {directory_id}')
        return files
