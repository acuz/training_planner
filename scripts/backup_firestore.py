"""
Firestore → Google Drive daily backup.
Reads all athlete data (subcollections under users/{uid}) and uploads
a dated JSON file to a specified Google Drive folder.

Required env vars:
  GOOGLE_SERVICE_ACCOUNT  - content of the service account JSON key file
  DRIVE_FOLDER_ID         - ID of the target Google Drive folder
"""

import io
import json
import os
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


def export_subcollection(ref):
    docs = {}
    for doc in ref.stream():
        docs[doc.id] = doc.to_dict()
    return docs


def export_all(db):
    backup = {}
    users_snap = db.collection("users").stream()
    for user_doc in users_snap:
        uid = user_doc.id
        user_data = user_doc.to_dict()
        user_ref = db.collection("users").document(uid)
        for sub_name in ["sessions", "availability", "weeks", "tests"]:
            user_data[sub_name] = export_subcollection(user_ref.collection(sub_name))
        backup[uid] = user_data
    return backup


def main():
    raw = os.environ["GOOGLE_SERVICE_ACCOUNT"]
    cred_dict = json.loads(raw)
    folder_id = os.environ["DRIVE_FOLDER_ID"]

    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    print("Exporting Firestore data…")
    backup = export_all(db)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"firestore-backup-{today}.json"
    content = json.dumps(backup, indent=2, default=str).encode("utf-8")
    print(f"  {sum(len(v.get('sessions', {})) for v in backup.values())} total sessions across {len(backup)} users")

    drive_creds = service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    service = build("drive", "v3", credentials=drive_creds)

    # Delete previous backup for today if it exists (avoid duplicates)
    existing = service.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
        fields="files(id)",
    ).execute().get("files", [])
    for f in existing:
        service.files().delete(fileId=f["id"]).execute()

    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype="application/json")
    result = service.files().create(body=file_metadata, media_body=media, fields="id,name").execute()
    print(f"Uploaded: {result['name']} (id={result['id']})")


if __name__ == "__main__":
    main()
