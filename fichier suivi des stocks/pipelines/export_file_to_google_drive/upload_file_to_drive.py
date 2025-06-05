import os

from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from openhexa.sdk import workspace

conn = workspace.custom_connection("credentials-api-google-drive")

scopes = ["https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_info(eval(conn.credentials), scopes=scopes)

service = build("drive", "v3", credentials=creds)


def upload_file(file_path, parent_id):
    """
    Ce programme sera principalement utilisé pour exporter le fichier généré dans un repertoire drive partagé.
    """
    mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    file_name = os.path.basename(file_path)

    file_metadata = {"name": file_name, "parents": [parent_id]}
    try:
        media = MediaFileUpload(file_path, mimetype=mimetype)
        file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()

    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None

    return file.get("id")


def get_share_link(file_id):
    "Permet d'obtenir le lien partagé du fichier"
    request_body = {"role": "reader", "type": "anyone"}
    try:
        _response_permissions = (
            service.permissions().create(fileId=file_id, body=request_body).execute()
        )
        response_share_link = service.files().get(fileId=file_id, fields="webViewLink").execute()
    except HttpError as error:
        print(f"An error occurred: {error}")
        response_share_link = None

    return response_share_link.get("webViewLink")


def check_if_folder_exist(date_report, parent_id):
    import pandas as pd

    try:
        # Check if directory exist
        year = str(pd.to_datetime(date_report).year)
        query = f"name='{year}' and parents='{parent_id}' and mimeType='application/vnd.google-apps.folder'"
        response = service.files().list(q=query, fields="files(id)").execute()
        files = response.get("files", [])

        # If folder does not exist, create it
        if not files:
            file_metadata = {
                "name": year,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_id],
            }
            file = service.files().create(body=file_metadata, fields="id").execute()
            return file.get("id")

        # Return the ID of the existing folder
        return files[0]["id"]

    except HttpError as error:
        print(f"An error occurred: {error}")


def delete_file_if_exist(service, file_path):
    try:
        file_name = os.path.basename(file_path)
        query = f"name='{file_name}'"
        results = service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
        files = results.get("files", [])

        if files:
            for file in files:
                print(f"Fichier trouvé : {file['name']} (ID: {file['id']})")
                # Supprimer le file
                service.files().delete(fileId=file["id"]).execute()
                print(f"Fichier supprimé : {file['name']}")

        else:
            print("Aucun file trouvé avec ce nom.")
    except HttpError as error:
        print(f"Une erreur s'est produite : {error}")


def upload_and_return_link(
    file_path, date_report, parent_id="1LHsczOhs4f5Z4DZkuMiiYGiwj_ppKb_Z", method="download"
):
    assert method in ("share", "download"), (
        "Le paramètre ne prend que les valeurs ('share', 'download')"
    )

    sub_folder_id = check_if_folder_exist(date_report, parent_id)
    delete_file_if_exist(service, file_path)
    file_id = upload_file(file_path, sub_folder_id)

    if method == "download":
        get_share_link(file_id)
        return f"https://drive.google.com/uc?id={file_id}"
    return get_share_link(file_id)
