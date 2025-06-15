import os, io, json, time, hashlib, asyncio
from tempfile import NamedTemporaryFile

from dbfread import DBF
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.api_core.exceptions import ResourceExhausted

import firebase_admin
from firebase_admin import credentials, firestore

# ───────────────────────── Configura aquí ─────────────────────────
FOLDER_ID       = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"  # Carpeta con los .DBF
PER_DOC_PAUSE   = 0.05                                  # Pausa entre docs
# ──────────────────────────────────────────────────────────────────


# ► Autenticación Google Drive
DRIVE_KEY = json.loads(os.environ["DRIVE_KEY"])
drive_creds = service_account.Credentials.from_service_account_info(
    DRIVE_KEY,
    scopes=["https://www.googleapis.com/auth/drive.readonly"],
)
drive = build("drive", "v3", credentials=drive_creds)

# ► Autenticación Firebase
FIRE_KEY = json.loads(os.environ["FIREBASE_KEY"])
cred_fb = credentials.Certificate(FIRE_KEY)
firebase_admin.initialize_app(cred_fb)
db = firestore.client()


# ─────────────────────── utilidades ───────────────────────────────

def list_dbf_files():
    query = (f"'{FOLDER_ID}' in parents and "
             "mimeType!='application/vnd.google-apps.folder' "
             "and name contains '.DBF'")
    files = drive.files().list(q=query, fields="files(id,name)").execute()["files"]
    return [f for f in files if f["name"].lower().endswith(".dbf")]


def download_file_to_tmp(file_id: str, file_name: str, retries: int = 3) -> str:
    """Descarga el .DBF a un archivo temporal y devuelve su ruta"""
    for attempt in range(1, retries + 1):
        try:
            request = drive.files().get_media(fileId=file_id)
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            buffer.seek(0)
            tmp = NamedTemporaryFile(delete=False, suffix=".dbf")
            tmp.write(buffer.read())
            tmp.close()
            return tmp.name
        except Exception as e:
            print(f"⚠️ Descarga fallida '{file_name}' intento {attempt}/3: {e}")
            time.sleep(2)
    raise Exception(f"No se pudo descargar '{file_name}' tras {retries} intentos")


def hash_record(rec: dict) -> str:
    return hashlib.sha1(json.dumps(rec, sort_keys=True).encode()).hexdigest()


async def upload_records(collection: str, rows, key_field: str):
    col = db.collection(collection)
    nuevos = act = skip = 0

    for rec in rows:
        doc_id = str(rec.get(key_field, "")).strip()
        if not doc_id:
            continue
        try:
            doc = col.document(doc_id)
            snap = doc.get()
            if not snap.exists:
                doc.set(rec)
                nuevos += 1
            else:
                if hash_record(snap.to_dict()) != hash_record(rec):
                    doc.set(rec)
                    act += 1
                else:
                    skip += 1
            await asyncio.sleep(PER_DOC_PAUSE)
        except ResourceExhausted:
            print("⏳ Cuota alcanzada. Esperando 10 s…")
            await asyncio.sleep(10)
        except Exception as e:
            print(f"⚠️ Error doc '{doc_id}' en '{collection}': {e}")
            await asyncio.sleep(1)

    print(f"✅ '{collection}': nuevos={nuevos}, actualizados={act}, sin cambios={skip}")


# ────────────────────────── MAIN ─────────────────────────────────

def main():
    print("⏳ Iniciando sincronización Firestore (uno por uno)…")
    files = list_dbf_files()
    print(f"🗂 Encontrados: {len(files)} archivos .DBF")

    for f in files:
        name = f["name"]
        coll = os.path.splitext(name)[0].lower()
        print(f"📂 {name} → colección '{coll}'")
        tmp_path = None
        try:
            tmp_path = download_file_to_tmp(f["id"], name)

            table = DBF(
                tmp_path,
                load=True,
                ignore_missing_memofile=True,
                encoding="latin1",
            )

            if len(table) == 0:
                print(f"⚠️ '{name}' está vacío. Se omite.")
                continue

            key_field = table.field_names[0].lower()
            print(f"🔑 Usando '{key_field}' como clave")

            rows = [
                {k.lower(): (str(v) if v is not None else None) for k, v in r.items()}
                for r in table
            ]

            asyncio.run(upload_records(coll, rows, key_field))

        except Exception as e:
            print(f"❌ Error procesando '{name}': {e}")
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    print("✅ Sincronización COMPLETADA")


if __name__ == "__main__":
    main()
