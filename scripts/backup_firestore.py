"""
Firestore → local JSON backup.
Reads all athlete data (subcollections under users/{uid}) and writes
a dated JSON file to the path specified by BACKUP_PATH env var.

Required env vars:
  GOOGLE_SERVICE_ACCOUNT  - content of the service account JSON key file
  BACKUP_PATH             - output file path (default: firestore-backup.json)
"""

import json
import os
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials, firestore


def export_subcollection(ref):
    docs = {}
    for doc in ref.stream():
        docs[doc.id] = doc.to_dict()
    return docs


def export_all(db):
    backup = {}
    for user_doc in db.collection("users").stream():
        uid = user_doc.id
        user_data = user_doc.to_dict()
        user_ref = db.collection("users").document(uid)
        for sub in ["sessions", "availability", "weeks", "tests"]:
            user_data[sub] = export_subcollection(user_ref.collection(sub))
        backup[uid] = user_data
    return backup


def main():
    cred_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
    out_path = os.environ.get("BACKUP_PATH", "firestore-backup.json")

    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    print("Exporting Firestore data…")
    backup = export_all(db)
    total_sessions = sum(len(v.get("sessions", {})) for v in backup.values())
    print(f"  {total_sessions} sessions across {len(backup)} users")

    os.makedirs(os.path.dirname(out_path) if os.path.dirname(out_path) else ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(backup, f, indent=2, default=str)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
