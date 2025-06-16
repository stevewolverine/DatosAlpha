"""
etl_firestore_incremental.py
-------------------------------------------------
Sincroniza .DBF de Google Drive con Firestore SOLO cuando:

1. El archivo ha cambiado en las últimas N horas.
2. Un registro cambió (comparando un hash SHA-1 guardado en el campo 'h').

• 1 lectura por documento (lee solo el campo 'h')
• 1 escritura SOLAMENTE si el hash difiere o el doc no existe
• Lotes de 400 escrituras, con pausa de 1 s entre lotes
• Requiere plan Blaze (sin límite de 20 000 ops/día)

Variables de entorno:
  DRIVE_KEY    → JSON service-account con acceso readonly a Drive
  FIREBASE_KEY → JSON service-account de Firebase
Ajusta:
  FOLDER_ID        → carpeta en Drive donde están los .DBF
  HOURS_WINDOW     → cuántas horas atrás considerar “cambiado”
  BATCH_SIZE       → máx. docs por commit (≤ 500)
"""

import os, io, json, time, hashlib
from datetime import datetime, timedelta, timezone
from tempfile import NamedTemporaryFile

from dbfread import DBF
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.api_core.exceptions import ResourceExhausted
import firebase_admin
from firebase_admin import credentials, firestore

# ───────── AJUSTES ─────────
FOLDER_ID    = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"
HOURS_WINDOW = 5            # procesa solo .DBF modificados en las últimas N h
BATCH_SIZE   = 400          # ≤ 500
PAUSE_SEC    = 1            # pausa entre commits
ENCODING     = "latin1"     # ajustar si tus DBF usan otro encoding
# ───────────────────────────

# --- Credenciales Drive ---
drive_creds = service_account.Credentials.from_service_account_info(
    json.loads(os.environ["DRIVE_KEY"]),
    scopes=["https://www.googleapis.com/auth/drive.readonly"],
)
drive = build("drive", "v3", credentials=drive_creds)

# --- Credenciales Firebase ---
firebase_creds = credentials.Certificate(json.loads(os.environ["FIREBASE_KEY"]))
firebase_admin.initialize_app(firebase_creds)
db = firestore.client()

# ------------------------------------------------------------------

def list_recent_dbf(hours_window):
    """Lista archivos .DBF modificados en las últimas N horas."""
    threshold = datetime.now(timezone.utc) - timedelta(hours=hours_window)
    q = (f"'{FOLDER_ID}' in parents and "
         "mimeType!='application/vnd.google-apps.folder' and "
         "name contains '.DBF'")
    fields = "files(id,name,modifiedTime)"
    files  = drive.files().list(q=q, fields=fields).execute()["files"]
    recent = []
    for f in files:
        mod = datetime.fromisoformat(f["modifiedTime"].rstrip('Z') + "+00:00")
        if mod > threshold and f["name"].lower().endswith(".dbf"):
            recent.append(f)
    return recent

def download_to_tmp(file_id, fname):
    """Descarga el archivo de Drive a un .dbf temporal y devuelve la ruta."""
    buf = io.BytesIO()
    MediaIoBaseDownload(buf, drive.files().get_media(fileId=file_id)).next_chunk()
    buf.seek(0)
    tmp = NamedTemporaryFile(delete=False, suffix=".dbf")
    tmp.write(buf.read()); tmp.close()
    return tmp.name

def sha1_dict(d):
    """Hash estable de un dict."""
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

# ------------------------------------------------------------------

print("⏳ Buscando archivos .DBF recientes…")
files = list_recent_dbf(HOURS_WINDOW)
print(f"🗂 {len(files)} archivos a procesar (últimas {HOURS_WINDOW} h)")

for f in files:
    name = f["name"]; col_name = name.rsplit('.',1)[0].lower()
    print(f"\n📂 {name} → colección '{col_name}'")

    try:
        tmp_path = download_to_tmp(f["id"], name)
        table = DBF(tmp_path, load=True,
                    ignore_missing_memofile=True, encoding=ENCODING)
        if len(table) == 0:
            print("⚠️  Vacío, omitido."); os.remove(tmp_path); continue

        key_field = table.field_names[0].lower()
        col_ref   = db.collection(col_name)
        batch     = db.batch(); n_batch = 0; escritos = 0; omitidos = 0

        for rec in table:
            doc = {k.lower(): (str(v) if v is not None else None)
                   for k, v in rec.items()}
            doc_id = str(doc[key_field]).strip()
            if not doc_id:   # sin clave -> se omite
                continue

            doc["h"] = sha1_dict(doc)         # hash de cambios

            try:
                snap = col_ref.document(doc_id).get(field_paths=["h"])
                if snap.exists and snap.get("h") == doc["h"]:
                    omitidos += 1
                    continue  # sin cambios -> ni escribir
            except Exception:
                pass          # doc nuevo o error -> escribir

            batch.set(col_ref.document(doc_id), doc)
            n_batch += 1; escritos += 1

            if n_batch == BATCH_SIZE:
                batch.commit(); batch = db.batch(); n_batch = 0
                time.sleep(PAUSE_SEC)

        if n_batch:
            batch.commit()

        os.remove(tmp_path)
        print(f"✅ {escritos} escritos, {omitidos} sin cambios")

    except ResourceExhausted:
        print("🚨 Se alcanzó el límite de escritura. Pausando 30 s…")
        time.sleep(30)
    except Exception as e:
        print(f"❌ Error procesando '{name}': {e}")

print("\n🎉 Sincronización COMPLETA")
