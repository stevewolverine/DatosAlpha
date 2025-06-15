"""
etl_firestore_one_by_one.py
-------------------------------------------------
Sincroniza todos los .DBF de una carpeta de Google Drive a Firebase Firestore,
cargando **registro por registro** (uno a uno) para evitar sobrepasar cuotas.

• Usa la primera columna del .DBF como clave (document ID).
• Descarga el .DBF a un archivo temporal en /tmp para que dbfread lo procese.
• Maneja reintentos y back-off si se agota la cuota (error 429).

Variables de entorno necesarias:
  DRIVE_KEY    → JSON del service-account con permiso de lectura en Drive
  FIREBASE_KEY → JSON del service-account de Firebase

Ajusta:
  FOLDER_ID  → ID de la carpeta en Drive que contiene los .DBF
  PER_DOC_PAUSE → pausa (segundos) entre documentos para no disparar cuotas
"""

import os, io, json, time, hashlib, asyncio, tempfile
from tempfile import NamedTemporaryFile

from dbfread import DBF
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.api_core.exceptions import ResourceExhausted

import firebase_admin
from firebase_admin import credentials, firestore

# ───────────────────────── Configura aquí ─────────────────────────
FOLDER_ID       = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"
PER_DOC_PAUSE   = 0.05      # segundos a dormir entre documentos
# ──────────────────────────────────────────────────────────────────


# ► Autenticación Google Drive
drive_creds = service_account.Credentials.from_service_account_info(
    json.loads(os.environ["DRIVE_KEY"]),
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=drive_creds)

# ► Autenticación Firebase
firebase_creds = credentials.Certificate(json.loads(os.environ["FIREBASE_KEY"]))
firebase_admin.initialize_app(firebase_creds)
db = firestore.client()


# ─────────────────────── utilidades ───────────────────────────────
def list_dbf_files():
    q = (f"'{FOLDER_ID}' in parents "
         "and mimeType!='application/vnd.google-apps.folder' "
         "and name contains '.DBF'")
    files = drive.files().list(q=q, fields="files(id,name)").execute()["files"]
    # Filtra solo extensión .dbf real
    return [f for f in files if f["name"].lower().endswith(".dbf")]


def download_file_to_tmp(file_id, file_name, retries=3) -> str:
    """
    Descarga el archivo de Drive a un fichero temporal .dbf y devuelve su ruta.
    """
    for attempt in range(1, retries + 1):
        try:
            request = drive.files().get_media(fileId=file_id)
            mem_buf = io.BytesIO()
            downloader = MediaIoBaseDownload(mem_buf, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            mem_buf.seek(0)
            tmp = NamedTemporaryFile(delete=False, suffix=".dbf")
            tmp.write(mem_buf.read())
            tmp.close()
            return tmp.name
        except Exception as e:
            print(f"⚠️ Descarga fallida '{file_name}' intento {attempt}/{retries}: {e}")
            time.sleep(2)
    raise Exception(f"No se pudo descargar '{file_name}' tras {retries} intentos")


def hash_record(rec: dict) -> str:
    return hashlib.sha1(json.dumps(rec, sort_keys=True).encode()).hexdigest()


async def upload_records(collection_name: str, records, key_field: str):
    """
    Inserta/actualiza documentos uno a uno con pequeña pausa para respetar cuota.
    """
    col_ref = db.collection(collection_name)
    nuevos = act = skip = 0

    for rec in records:
        try:
            doc_id = str(rec.get(key_field, "")).strip()
            if not doc_id:
                continue

            doc_ref = col_ref.document(doc_id)
            snap = doc_ref.get()
            if not snap.exists:
                doc_ref.set(rec)
                nuevos += 1
            else:
                if hash_record(snap.to_dict()) != hash_record(rec):
                    doc_ref.set(rec)
                    act += 1
                else:
                    skip += 1
            await asyncio.sleep(PER_DOC_PAUSE)
        except ResourceExhausted:
            print("⏳ Cuota de escritura alcanzada. Esperando 10 s…")
            await asyncio.sleep(10)
        except Exception as e:
            print(f"⚠️ Error doc '{doc_id}' en '{collection_name}': {e}")
            await asyncio.sleep(1)

    print(f"✅ '{collection_name}': nuevos={nuevos}, actualizados={act}, sin cambios={skip}")


# ────────────────────────── MAIN ─────────────────────────────────
def main():
    print("⏳ Iniciando sincronización Firestore (uno por uno)…")
    files = list_dbf_files()
    print(f"🗂 Encontrados: {len(files)} archivos .DBF")

    for f in files:
        name = f["name"]
        coll = os.path.splitext(name)[0].lower()
        print(f"📂 {name} → colección '{coll}'")

        try:
            tmp_path = download_file_to_tmp(f["id"], name)
table = DBF(
    tmp_path,
    load=True,                       # carga en memoria para que len() funcione
    ignore_missing_memofile=True,
    encoding="latin1"
)

if len(table) == 0:                  # ⬅️ uso de len()
    print(f"⚠️ '{name}' está vacío, se omite.")
    os.remove(tmp_path)
    continue

            key_field = table.field_names[0].lower()
            print(f"🔑 Usando '{key_field}' como clave")

            # Convierte registros a dict con claves en minúsculas
            registros = [
                {k.lower(): (str(v) if v is not None else None) for k, v in rec.items()}
                for rec in table
            ]

            asyncio.run(upload_records(coll, registros, key_field))
        except Exception as e:
            print(f"❌ Error procesando '{name}': {e}")
        finally:
            # limpia temporal si existe
            try:
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    print("✅ Sincronización COMPLETADA")


if __name__ == "__main__":
    main()

