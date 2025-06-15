import os
import io
import time
import asyncio
import firebase_admin
from firebase_admin import credentials, firestore
from google.oauth2 import service_account
from google.cloud.firestore_v1 import Client
from dbfread import DBF
from google.auth.transport.requests import Request
from google.api_core.exceptions import ResourceExhausted
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth import default

# Configuración de Firestore
cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred)
db: Client = firestore.client()

# ID de la carpeta en Google Drive
FOLDER_ID = '1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL'

# Inicializa API de Google Drive
def get_drive_service():
    creds, _ = default(scopes=['https://www.googleapis.com/auth/drive'])
    return build('drive', 'v3', credentials=creds)

# Descarga el archivo de Drive a memoria
def download_file(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh

# Obtiene todos los archivos .DBF de la carpeta de Drive
def list_dbf_files(service):
    query = f"'{FOLDER_ID}' in parents and mimeType != 'application/vnd.google-apps.folder' and name contains '.DBF'"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

# Detecta la clave primaria automáticamente (primera columna del primer registro)
def get_primary_key(record):
    if not record:
        return 'id'
    return list(record.keys())[0].lower()

# Carga registros uno por uno con control de errores y reintentos asincrónicos
async def upload_records_one_by_one(collection_name, records, key_field):
    for record in records:
        try:
            doc_id = str(record.get(key_field, '')).strip()
            if not doc_id:
                continue
            doc_ref = db.collection(collection_name).document(doc_id)
            snapshot = doc_ref.get()
            if not snapshot.exists or snapshot.to_dict() != record:
                doc_ref.set(record)
            await asyncio.sleep(0.05)  # pequeño retraso entre documentos
        except Exception as e:
            print(f"⚠️ Error al guardar documento '{doc_id}' en colección '{collection_name}': {e}")
            await asyncio.sleep(1)

# Proceso principal
def main():
    print("⏳ Iniciando sincronización con Firestore...")
    service = get_drive_service()
    files = list_dbf_files(service)
    print(f"🗂 Archivos .DBF encontrados: {len(files)}")

    for file in files:
        name = file['name']
        file_id = file['id']
        collection_name = os.path.splitext(name)[0].lower()

        for attempt in range(1, 4):
            try:
                print(f"📂 Procesando: {name} → colección '{collection_name}'")
                fh = download_file(service, file_id)
                table = DBF(fh, ignore_missing_memofile=True, encoding='latin1')
                records = list(table)
                key_field = get_primary_key(records[0] if records else {})
                print(f"🔑 Usando '{key_field}' como clave para colección '{collection_name}'")
                asyncio.run(upload_records_one_by_one(collection_name, records, key_field))
                print(f"✅ {len(records)} registros cargados en colección '{collection_name}'")
                break  # salir del ciclo de intentos si se completa correctamente
            except ResourceExhausted:
                wait_time = 5 * attempt
                print(f"❌ Error al procesar '{name}' (intento {attempt}/3): cuota excedida. Esperando {wait_time} segundos...")
                time.sleep(wait_time)
            except Exception as e:
                print(f"❌ Error al procesar '{name}' (intento {attempt}/3): {e}")
                time.sleep(5)

    print("✅ Sincronización completada.")

if __name__ == '__main__':
    main()
