# etl_firestore.py – Carga archivos .DBF desde Google Drive a Firebase Firestore
# Requiere: dbfread, google-api-python-client, firebase-admin
# Variables de entorno: DRIVE_KEY y FIREBASE_KEY (como JSON)

import os, json, io
from dbfread import DBF
from googleapiclient.discovery import build
from google.oauth2 import service_account
import firebase_admin
from firebase_admin import credentials, firestore

# ----- CONFIGURACIÓN -----
FOLDER_ID = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"  # ← REEMPLAZA por el ID real de tu carpeta en Drive

# ----- AUTENTICACIÓN GOOGLE DRIVE -----
drive_creds = service_account.Credentials.from_service_account_info(
    json.loads(os.environ["DRIVE_KEY"]),
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=drive_creds)

# ----- AUTENTICACIÓN FIREBASE -----
firebase_creds = credentials.Certificate(json.loads(os.environ["FIREBASE_KEY"]))
firebase_admin.initialize_app(firebase_creds)
db = firestore.client()

# ----- FUNCIONES -----
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

    count = 0
    for record in dbf:
        doc = {k.lower(): str(v) if v is not None else None for k, v in record.items()}
        collection_ref.add(doc)
        count += 1

    print(f"✅ {count} registros cargados en colección '{collection_name}'")

# ----- MAIN -----
def main():
    print("⏳ Iniciando sincronización con Firestore...")
    try:
        dbf_files = list_dbf_files()
        print(f"🗂 Archivos .DBF encontrados: {len(dbf_files)}")

        if not dbf_files:
            print("⚠️ No se encontraron archivos .DBF en la carpeta de Drive.")
            print("🔍 Verifica que el FOLDER_ID sea correcto y que la carpeta esté compartida con el service account.")
            return

        for f in dbf_files:
            name = f["name"].rsplit(".", 1)[0].lower()
            print(f"📂 Procesando: {f['name']} → colección '{name}'")
            buf = download_file(f["id"])
            load_dbf_to_firestore(buf, name)

        print("✅ Sincronización completada exitosamente.")

    except Exception as e:
        print("❌ Error detectado:", str(e))

if __name__ == "__main__":
    main()

