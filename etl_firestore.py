#!/usr/bin/env python3
"""
Sincroniza archivos .DBF en Google Drive → Firebase Firestore
- Solo procesa archivos modificados en las últimas HOURS_WINDOW horas
- Sube solo registros nuevos o modificados (hash SHA-1)
- Usa lote de escrituras (≤ BATCH_SIZE) con reintentos
- El campo que se usa como ID se define por tabla en KEY_FIELD
Requiere:
  pip install dbfread google-api-python-client google-auth google-auth-httplib2 \
              google-auth-oauthlib firebase-admin python-dateutil
Variables de entorno:
  DRIVE_KEY    → credenciales de servicio (JSON) con acceso de solo lectura a Drive
  FIREBASE_KEY → credenciales de servicio (JSON) del proyecto Firebase
"""

import os, io, json, time, hashlib
from datetime import datetime, timedelta, timezone
from tempfile import NamedTemporaryFile
from dateutil import parser as dtparse

from dbfread import DBF
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.api_core.exceptions import ResourceExhausted
import firebase_admin
from firebase_admin import credentials, firestore

# ───────── AJUSTES GENERALES ─────────
FOLDER_ID    = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"
HOURS_WINDOW = 5                # procesa solo archivos modificados en las últimas N horas
BATCH_SIZE   = 400              # escrituras por commit (≤500)
PAUSE_SEC    = 1                # pausa entre commits (seg)
ENCODING     = "latin1"         # codificación de los DBF
# ─────────────────────────────────────

# ───────── MAPEO DE CLAVES POR TABLA ─────────
#  nombre de la colección (sin .dbf, minúsculas)  :  nombre del campo clave en el DBF
KEY_FIELD = {
    "producto"   : "CVE_PROD",
    "clientes"   : "CVE_CTE",
    "creditod"   : "NO_NOTA",
    "creditos"   : "NO_NOTA",
    "existe"     : "CVE_PROD",
    "factentr"   : "NO_FAC",
    "facturac"   : "NO_FAC",
    "facturad"   : "NO_FAC",
    "precioprod" : "CVE_PROD",
    "prod_desc"  : "CVE_PROD",
    "prodimag"   : "CVE_PROD",
}
# Si una tabla no aparece aquí, se usará la primera columna del DBF
# ──────────────────────────────────────────────

# ───────── CONEXIÓN A GOOGLE DRIVE ────────────
creds_drive = service_account.Credentials.from_service_account_info(
    json.loads(os.environ["DRIVE_KEY"]),
    scopes=["https://www.googleapis.com/auth/drive.readonly"],
)
drive = build("drive", "v3", credentials=creds_drive)

# ───────── CONEXIÓN A FIREBASE ────────────────
creds_fb = credentials.Certificate(json.loads(os.environ["FIREBASE_KEY"]))
firebase_admin.initialize_app(creds_fb)
db = firestore.client()

# ───────────────── FUNCIONES ──────────────────
def safe_commit(batch, retries=3):
    for n in range(retries):
        try:
            batch.commit()
            return
        except Exception as e:
            wait = 5 * (n + 1)
            print(f"⏳ Commit falló ({n+1}/{retries}) → esperando {wait}s: {e}")
            time.sleep(wait)
    raise RuntimeError("Commit fallido tras reintentos")

def list_recent_dbf(hours_window):
    """Devuelve archivos .DBF modificados dentro de hours_window horas."""
    threshold = datetime.now(timezone.utc) - timedelta(hours=hours_window)
    q = (f"'{FOLDER_ID}' in parents and "
         "mimeType!='application/vnd.google-apps.folder' and name contains '.DBF'")
    fields = "files(id,name,modifiedTime)"
    files  = drive.files().list(q=q, fields=fields).execute()["files"]
    recent = []
    print("📑 DEBUG listado de archivos y fechas:")
    for f in files:
        mod = dtparse.isoparse(f["modifiedTime"])   # hora UTC
        print(f"   • {f['name']:20}  mod={mod}  >? umbral={threshold}")
        if hours_window >= 876000 or mod > threshold:   # 100 años ⇒ forzar full
            recent.append(f)
    return recent

def download_to_tmp(file_id):
    buf = io.BytesIO()
    MediaIoBaseDownload(buf, drive.files().get_media(fileId=file_id)).next_chunk()
    buf.seek(0)
    tmp = NamedTemporaryFile(delete=False, suffix=".dbf")
    tmp.write(buf.read()); tmp.close()
    return tmp.name

def sha1_dict(d):
    """Hash SHA-1 estable del dict (para detectar cambios)."""
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

# ─────────────────────────── MAIN ───────────────────────────────
if __name__ == "__main__":
    print("⏳ Buscando archivos .DBF recientes…")
    files = list_recent_dbf(HOURS_WINDOW)
    print(f"🗂 A procesar: {len(files)} archivos (últimas {HOURS_WINDOW} h)\n")

    for f in files:
        name = f["name"]
        col_name = name.rsplit('.', 1)[0].lower()   # nombre de colección
        print(f"📂 {name} → '{col_name}'")

        try:
            tmp_path = download_to_tmp(f["id"])
            table = DBF(tmp_path, load=True, ignore_missing_memofile=True, encoding=ENCODING)
            if len(table) == 0:
                print("⚠️  Vacío, omitido"); os.remove(tmp_path); continue

            # Selección de la columna clave
            key_field = KEY_FIELD.get(col_name, table.field_names[0]).upper()

            col   = db.collection(col_name)
            batch = db.batch()
            cnt = writes = skips = 0

            for rec in table:
                # Lowercase keys + cast a string, None queda None
                doc = {k.lower(): (str(v) if v is not None else None) for k, v in rec.items()}

                doc_id = str(rec[key_field]).strip()
                if not doc_id:
                    continue   # ignora registros sin clave

                doc["h"] = sha1_dict(doc)

                # Verifica si el documento cambió
                try:
                    h_snap = col.document(doc_id).get(field_paths=["h"])
                    if h_snap.exists and h_snap.get("h") == doc["h"]:
                        skips += 1
                        continue
                except Exception:
                    pass

                batch.set(col.document(doc_id), doc)
                cnt += 1; writes += 1
                if cnt == BATCH_SIZE:
                    safe_commit(batch)
                    batch = db.batch()
                    cnt = 0
                    time.sleep(PAUSE_SEC)

            if cnt:
                safe_commit(batch)

            print(f"✅ escritos={writes}, saltados={skips}\n")
            os.remove(tmp_path)

        except ResourceExhausted:
            print("🚨 Cuota Firestore alcanzada, pausa 30 s"); time.sleep(30)
        except Exception as e:
            print(f"❌ Error en '{name}': {e}")

    print("🎉 Sincronización COMPLETA")
