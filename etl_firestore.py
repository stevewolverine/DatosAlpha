# etl_firestore.py
import os, json, io
from dbfread import DBF
from googleapiclient.discovery import build
from google.oauth2 import service_account
import firebase_admin
from firebase_admin import credentials, firestore

# ----- Google Drive -----
drive_creds = service_account.Credentials.from_service_account_info(
    json.loads(os.environ["DRIVE_KEY"]),
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=drive_creds)

FOLDER_ID = "TU_ID_DE_CARPETA_EN_DRIVE"  # <- Cámbialo por el real

# ----- Firebase -----
firebase_creds = credentials.Certificate(json.loads(os.environ["FIREBASE_KEY"]))
firebase_admin.initialize_app(firebase_creds)
db = firestore.client()

def list_dbf_files():
    q = f"'{FOLDER_ID}' in parents and mimeType != 'application/vnd.google-apps.folder'"
    files = drive.files().list(q=q, fields="files(id,name)").execute()["files"]
    return [f for f in files if f["name"].lower().endswith(".dbf")]

def download_file(file_id):
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    status, body = drive._http.request(request.uri)
    buf.write(body)
    buf.seek(0)
    return buf

def load_dbf_to_firestore(buf, collection_name):
    dbf = DBF(buf, load=True)
    collection_ref = db.collection(collection_name)

    print(f"Subiendo a colección: {collection_name}")
    for record in dbf:
        doc = {k.lower(): str(v) if v is not None else None for k, v in record.items()}
        collection_ref.add(doc)
    print(f"✓ {collection_name} listo.")

def main():
    for f in list_dbf_files():
        name = f["name"].rsplit(".", 1)[0].lower()
        print("Procesando:", f["name"])
        buf = download_file(f["id"])
        load_dbf_to_firestore(buf, name)

if __name__ == "__main__":
    main()
