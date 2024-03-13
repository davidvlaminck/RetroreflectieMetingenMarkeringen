import json
import socket
from io import FileIO
from pathlib import Path
from typing import Dict, Iterator

from apiclient import http
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload


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

    def list_files_in_directory(self, directory_id: str, recursive_level: int = 0, file_types: [str] = None) -> [Dict]:
        """
        Returns files in Google Drive folder given the id of the folder

        :param directory_id: the id of the Google Drive (see url after 'folders/')
        :param file_types: if not empty, this list will be used to filter the files by extension
        :param recursive_level: if not zero, this is the depth level of directories the function will search in. If -1,
        it will search recursively without a limit.
        :return: Returns an iterator of dictionaries, each with the id, name, parent_name and mimeType of the object
        """
        yield from self.list_objects_in_directory(directory_id, return_directories=False,
                                                  recursive_level=recursive_level, file_types=file_types)

    def list_directories_in_directory(self, directory_id: str, recursive_level: int = 0) -> [Dict]:
        """
        Returns directories in Google Drive folder given the id of the folder

        :param directory_id: the id of the Google Drive (see url after 'folders/')
        :param recursive_level: if not zero, this is the depth level of directories the function will search in. If -1,
        it will search recursively without a limit.
        :return: Returns an iterator of dictionaries, each with the id, name, parent_name and mimeType of the object
        """
        yield from self.list_objects_in_directory(directory_id, return_files=False, recursive_level=recursive_level)

    def list_objects_in_directory(self, directory_id: str, return_files: bool = True, file_types: [str] = None,
                                  return_directories: bool = True, recursive_level: int = 0) -> Iterator[Dict]:
        """
        Returns objects in Google Drive folder given the id of the folder

        :param directory_id: the id of the Google Drive (see url after 'folders/')
        :param return_files: if False this will exclude files, defaults to True
        :param return_directories: if False this will exclude directories, defaults to True
        :param file_types: if not empty, this list will be used to filter the files by extension
        :param recursive_level: if not zero, this is the depth level of directories the function will search in. If -1,
        it will search recursively without a limit.
        :return: Returns an iterator of dictionaries, each with the id, name, parent_name and mimeType of the object
        """
        creds = self.authenticate()

        try:
            service = build('drive', 'v3', credentials=creds)
            page_token = None
            while True:
                dir_name = service.files().get(fileId=directory_id, fields='name',
                                               supportsAllDrives=True).execute().get('name')
                response = service.files().list(q=f"'{directory_id}' in parents",
                                                fields='nextPageToken, '
                                                       'files(id, name, mimeType)',
                                                includeItemsFromAllDrives=True,
                                                supportsAllDrives=True,
                                                pageToken=page_token).execute()
                for drive_object in response.get('files', []):
                    drive_object['parent_name'] = dir_name
                    drive_object['parent_id'] = directory_id
                    mimetype = drive_object.get("mimeType")
                    if mimetype == 'application/vnd.google-apps.folder':
                        if return_directories:
                            yield drive_object
                        if recursive_level != 0:
                            yield from (self.list_objects_in_directory(
                                directory_id=drive_object.get('id'), return_files=return_files, file_types=file_types,
                                return_directories=return_directories, recursive_level=recursive_level - 1))
                    else:
                        if return_files:
                            if file_types:
                                extension = drive_object.get('name').split('.')[-1]
                                if extension in file_types:
                                    yield drive_object
                            else:
                                yield drive_object
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break

        except HttpError as error:
            print(F'An error occurred: {error}')
            yield None

    def find_directory_by_name(self, directory_name: str, directory_id: str = None) -> [str]:
        creds = self.authenticate()
        try:
            service = build('drive', 'v3', credentials=creds)
            query = f"mimeType='application/vnd.google-apps.folder' and name='{directory_name}'"
            if directory_id is not None:
                query += f" and '{directory_id}' in parents"
            response = service.files().list(q=query, includeItemsFromAllDrives=True, supportsAllDrives=True,
                                            fields='nextPageToken, files(id, name)').execute()
            return [d.get('id') for d in response.get('files', [])]
        except HttpError as error:
            print(F'An error occurred: {error}')
            return None

    def create_directory(self, directory_name: str, parent_id: str) -> str | None:
        creds = self.authenticate()
        try:
            service = build('drive', 'v3', credentials=creds)
            directory_metadata = {
                'name': directory_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            file = service.files().create(body=directory_metadata, fields='id', supportsAllDrives=True).execute()
            return file.get('id')
        except HttpError as error:
            print(F'An error occurred: {error}')
            return None

    def copy_file_to_dir(self, file_id: str, dir_id: str):
        creds = self.authenticate()
        copied_file = {'parents': [dir_id]}
        try:
            service = build('drive', 'v3', credentials=creds)
            return service.files().copy(fileId=file_id, body=copied_file, supportsAllDrives=True).execute()
        except HttpError as error:
            print(F'An error occurred: {error}')
            return None

    def download_file(self, file_id:str, file_path: Path) -> None:
        creds = self.authenticate()
        fh = FileIO(file_path, 'w+')

        try:
            service = build('drive', 'v3', credentials=creds)
            request = service.files().get_media(fileId=file_id)
            media_request = http.MediaIoBaseDownload(fh, request)

            while True:
                try:
                    download_progress, done = media_request.next_chunk()
                except Exception as exc:
                    print('An error occurred: %s' % exc)
                    return
                if download_progress:
                    print('Download Progress: %d%%' % int(download_progress.progress() * 100))
                if done:
                    print(f'Download of {file_path.name} Complete')
                    return
        except HttpError as error:
            print(F'An error occurred when downloading {file_path.name}: {error}')

    def upload_file(self, file_path: Path, dir_id: str, shared_drive_id: str = None) -> str:
        # upload the file at file_path to the Google Drive directory dir_id

        file_metadata = {
            'name': file_path.name,
            # 'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'parents': [dir_id]
        }

        if shared_drive_id is not None:
            file_metadata['driveId'] = shared_drive_id

        creds = self.authenticate()
        media = MediaFileUpload(file_path, resumable=True)

        try:
            service = build('drive', 'v3', credentials=creds)
            if shared_drive_id is None:
                file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            else:
                file = service.files().create(body=file_metadata, media_body=media, fields='id',
                                              supportsAllDrives=True).execute()
            return file.get('id')

        except HttpError as error:
            print(F'An error occurred: {error}')
