import os, json, io, tempfile, hashlib, time
from dbfread import DBF
from googleapiclient.discovery import build
from google.oauth2 import service_account
import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

FOLDER_ID = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"  # ← Reemplaza con el ID de la carpeta de Drive

# --- Autenticación Google Drive ---
drive_creds = service_account.Credentials.from_service_account_info(
    json.loads(os.environ["DRIVE_KEY"]),
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=drive_creds)

# --- Autenticación Firebase ---
firebase_creds = credentials.Certificate(json.loads(os.environ["FIREBASE_KEY"]))
firebase_admin.initialize_app(firebase_creds)
db = firestore.client()

def list_dbf_files():
    q = f"'{FOLDER_ID}' in parents and mimeType != 'application/vnd.google-apps.folder'"
    files = drive.files().list(q=q, fields="files(id,name)").execute()["files"]
    return [f for f in files if f["name"].lower().endswith(".dbf")]

def download_file(file_id, file_name, retries=3):
    for attempt in range(1, retries + 1):
        try:
            request = drive.files().get_media(fileId=file_id)
            _, body = drive._http.request(request.uri)
            temp_path = os.path.join(tempfile.gettempdir(), file_name)
            with open(temp_path, "wb") as f:
                f.write(body)
            return temp_path
        except Exception as e:
            print(f"⚠️ Fallo al descargar '{file_name}' (intento {attempt}/{retries}): {e}")
            time.sleep(2)
    raise Exception(f"No se pudo descargar '{file_name}' tras {retries} intentos.")

def hash_record(data: dict):
    return hashlib.sha1(json.dumps(data, sort_keys=True).encode()).hexdigest()

def load_dbf_to_firestore(file_path, collection_name):
    dbf = DBF(file_path, load=True)
    collection_ref = db.collection(collection_name)
    key_field = dbf.field_names[0].lower()
    print(f"🔑 Usando '{key_field}' como clave para colección '{collection_name}'")

    count_new = 0
    count_updated = 0
    count_skipped = 0

    batch = db.batch()
    batch_count = 0

    for record in dbf:
        data = {k.lower(): str(v) if v is not None else None for k, v in record.items()}
        key = str(data.get(key_field))
        if not key:
            continue

        doc_ref = collection_ref.document(key)
        try:
            snapshot = doc_ref.get()
        except ResourceExhausted:
            print("⏳ Límite de lecturas alcanzado. Esperando 10 segundos...")
            time.sleep(10)
            snapshot = doc_ref.get()
        except Exception as e:
            print(f"⚠️ Error al leer documento '{key}' en colección '{collection_name}': {e}")
            continue

        if not snapshot.exists:
            batch.set(doc_ref, data)
            count_new += 1
        else:
            existing = snapshot.to_dict()
            if hash_record(existing) != hash_record(data):
                batch.set(doc_ref, data)
                count_updated += 1
            else:
                count_skipped += 1

        batch_count += 1
        time.sleep(0.01)

        if batch_count >= 200:
            try:
                batch.commit()
            except ResourceExhausted:
                print("⏳ Límite de escritura alcanzado. Esperando 10 segundos...")
                time.sleep(10)
                batch.commit()
            batch = db.batch()
            batch_count = 0
            time.sleep(2)

    if batch_count > 0:
        try:
            batch.commit()
        except ResourceExhausted:
            print("⏳ Límite de escritura alcanzado (final). Esperando 10 segundos...")
            time.sleep(10)
            batch.commit()

    print(f"✅ '{collection_name}': nuevos={count_new}, actualizados={count_updated}, sin cambios={count_skipped}")

def main():
    print("⏳ Iniciando sincronización con Firestore...")
    try:
        dbf_files = list_dbf_files()
        print(f"🗂 Archivos .DBF encontrados: {len(dbf_files)}")

        if not dbf_files:
            print("⚠️ No se encontraron archivos .DBF en la carpeta de Drive.")
            return

        for f in dbf_files:
            name = f["name"].rsplit(".", 1)[0].lower()
            print(f"📂 Procesando: {f['name']} → colección '{name}'")
            for intento in range(3):
                try:
                    temp_path = download_file(f["id"], f["name"])
                    load_dbf_to_firestore(temp_path, name)
                    break
                except Exception as e:
                    print(f"❌ Error al procesar '{f['name']}' (intento {intento+1}/3): {e}")
                    time.sleep(10)

        print("✅ Sincronización completada.")
    except Exception as e:
        print("❌ Error general:", str(e))

if __name__ == "__main__":
    main()

