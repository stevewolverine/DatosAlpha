import os, json, io, tempfile, hashlib, time, asyncio
from dbfread import DBF
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

# ----------------- CONFIG -----------------
FOLDER_ID = '1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL'  # carpeta con los .DBF en Drive

# ----------------- AUTH DRIVE -----------------
drive_creds = service_account.Credentials.from_service_account_info(
    json.loads(os.environ['DRIVE_KEY']),
    scopes=['https://www.googleapis.com/auth/drive.readonly']
)
drive = build('drive', 'v3', credentials=drive_creds)

# ----------------- AUTH FIREBASE -----------------
firebase_creds = credentials.Certificate(json.loads(os.environ['FIREBASE_KEY']))
firebase_admin.initialize_app(firebase_creds)
db = firestore.client()

# -------------- HELPERS -----------------

def list_dbf_files():
    q = f"'{FOLDER_ID}' in parents and mimeType != 'application/vnd.google-apps.folder' and name contains '.DBF'"
    res = drive.files().list(q=q, fields='files(id,name)').execute()
    return res.get('files', [])


def download_file(file_id, filename, retries=3):
    request = drive.files().get_media(fileId=file_id)
    for attempt in range(1, retries + 1):
        try:
            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            buf.seek(0)
            return buf
        except Exception as e:
            print(f"⚠️ Descarga fallida '{filename}' intento {attempt}/{retries}: {e}")
            time.sleep(3 * attempt)
    raise RuntimeError(f"No se pudo descargar '{filename}' tras {retries} intentos")


def hash_record(d: dict):
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()


async def upload_one_by_one(col_name, records, key_field):
    for rec in records:
        doc_id = str(rec.get(key_field, '')).strip()
        if not doc_id:
            continue
        ref = db.collection(col_name).document(doc_id)
        try:
            snap = ref.get()
        except ResourceExhausted:
            print('⏳ Límite lecturas, esperar 10s...')
            await asyncio.sleep(10)
            snap = ref.get()
        if not snap.exists or snap.to_dict() != rec:
            try:
                ref.set(rec)
            except ResourceExhausted:
                print('⏳ Límite escrituras, esperar 10s...')
                await asyncio.sleep(10)
                ref.set(rec)
        await asyncio.sleep(0.05)  # 50 ms entre docs


# -------------- MAIN -----------------

def main():
    print('⏳ Iniciando sincronización Firestore (uno por uno)...')
    files = list_dbf_files()
    print(f'🗂 Encontrados: {len(files)} archivos .DBF')

    for f in files:
        name = f['name']
        col = os.path.splitext(name)[0].lower()
        print(f"📂 {name} → colección '{col}'")
        try:
            buf = download_file(f['id'], name)
            table = DBF(buf, ignore_missing_memofile=True, encoding='latin1')
            recs = [ {k.lower(): (str(v) if v is not None else None) for k,v in r.items()} for r in table ]
            if not recs:
                print('⚠️ Archivo vacío, omitido.')
                continue
            key_f = list(recs[0].keys())[0]
            print(f"🔑 Clave primaria detectada: '{key_f}' | registros: {len(recs)}")
            asyncio.run(upload_one_by_one(col, recs, key_f))
            print(f"✅ Colección '{col}' finalizada\n")
        except Exception as e:
            print(f"❌ Error procesando '{name}': {e}\n")

    print('✅ Sincronización COMPLETADA')


if __name__ == '__main__':
    main()
