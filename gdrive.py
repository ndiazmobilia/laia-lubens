import io
import re
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from googleapiclient.http import MediaIoBaseDownload

# Scopes required for the actions. Read-only is sufficient.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = 'service_account.json'

# Define common MIME types for easier checking
MIME_TYPES = {
    'sheet': 'application/vnd.google-apps.spreadsheet',
    'excel': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'excel_legacy': 'application/vnd.ms-excel'
}



def get_drive_service() -> Resource | None:
    """
    Authenticates with the Google Drive API using a service account.

    :return: An authenticated Google Drive service resource object, or None if authentication fails.
    """
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        return service
    except FileNotFoundError:
        print(f"ERROR: The service account file '{SERVICE_ACCOUNT_FILE}' was not found.")
        return None
    except Exception as e:
        print(f"An error occurred during authentication: {e}")
        return None


def extract_folder_id_from_url(url: str) -> str | None:
    """
    Extracts the Google Drive folder ID from a URL.

    :param url: The full Google Drive folder URL.
    :return: The extracted folder ID, or None if the pattern doesn't match.
    """
    match = re.search(r'/folders/([a-zA-Z0-9_-]+)', url)
    if match:
        return match.group(1)
    return None


def list_files_in_folder(service: Resource, folder_id: str) -> list[dict]:
    """
    Lists all files within a specific Google Drive folder, sorted by creation time.

    :param service: The authenticated Google Drive service resource.
    :param folder_id: The ID of the folder to list files from.
    :return: A list of file objects (dictionaries), now including mimeType and createdTime.
    """
    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'"
    try:
        results = service.files().list(
            q=query,
            pageSize=100,  # Max 1000
            orderBy="createdTime asc",  # Sort by upload date in ascending order
            fields="nextPageToken, files(id, name, mimeType, createdTime)"
        ).execute()
        return results.get('files', [])
    except Exception as e:
        print(f"An error occurred while listing files: {e}")
        return []


def get_file_as_stream(service: Resource, file: dict) -> io.BytesIO | None:
    """
    Gets a file's content as an in-memory byte stream.

    It handles both native Google Sheets (by exporting them) and regular
    files like .xlsx (by downloading them).

    :param service: The authenticated Google Drive service resource.
    :param file: The file object, including 'id', 'name', and 'mimeType'.
    :return: A BytesIO object containing the file content, or None on failure.
    """
    file_id = file['id']
    file_mime_type = file['mimeType']
    request = None

    try:
        if file_mime_type == MIME_TYPES['sheet']:
            # It's a Google Sheet, so we need to export it
            print(f"Exporting Google Sheet: {file['name']}")
            request = service.files().export_media(fileId=file_id, mimeType=MIME_TYPES['excel'])
        elif file_mime_type in [MIME_TYPES['excel'], MIME_TYPES['excel_legacy']]:
            # It's a regular Excel file (.xlsx or .xls), so we can download it directly
            print(f"Downloading Excel file: {file['name']}")
            request = service.files().get_media(fileId=file_id)
        else:
            # Unsupported file type
            print(f"Skipping unsupported file type '{file_mime_type}' for file: {file['name']}")
            return None

        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        file_stream.seek(0)
        return file_stream

    except Exception as e:
        print(f"An error occurred while getting content for file {file_id}: {e}")
        return None
