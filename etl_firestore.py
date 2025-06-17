import os, io, json, time, hashlib
from datetime import datetime, timedelta, timezone
from tempfile import NamedTemporaryFile
from dateutil import parser as dtparse   # ← parse ISO8601 robusto

from dbfread import DBF
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.api_core.exceptions import ResourceExhausted
import firebase_admin
from firebase_admin import credentials, firestore

# ───────── AJUSTES ─────────
FOLDER_ID    = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"
HOURS_WINDOW = 5               # procesa sólo archivos modificados en las últimas N horas
BATCH_SIZE   = 400             # escrituras por commit (≤500)
PAUSE_SEC    = 1               # pausa entre commits
ENCODING     = "latin1"
# ───────────────────────────

# Credenciales Drive
creds_drive = service_account.Credentials.from_service_account_info(
    json.loads(os.environ["DRIVE_KEY"]),
    scopes=["https://www.googleapis.com/auth/drive.readonly"],
)
drive = build("drive", "v3", credentials=creds_drive)

# Credenciales Firebase
creds_fb = credentials.Certificate(json.loads(os.environ["FIREBASE_KEY"]))
firebase_admin.initialize_app(creds_fb)
db = firestore.client()

# ───────────────────────── funciones ────────────────────────────

def safe_commit(batch, retries=3):
    for n in range(retries):
        try:
            batch.commit(); return
        except Exception as e:
            wait = 5 * (n + 1)
            print(f"⏳ Commit falló ({n+1}/{retries}) → esperando {wait}s: {e}")
            time.sleep(wait)
    raise RuntimeError("Commit fallido tras reintentos")


def list_recent_dbf(hours_window):
    """Devuelve archivos .DBF modificados dentro de hours_window."""
    threshold = datetime.now(timezone.utc) - timedelta(hours=hours_window)
    q = (f"'{FOLDER_ID}' in parents and "
         "mimeType!='application/vnd.google-apps.folder' and name contains '.DBF'")
    fields = "files(id,name,modifiedTime)"
    files  = drive.files().list(q=q, fields=fields).execute()["files"]
    recent = []
    print("📑 DEBUG listado de archivos y fechas:")
    for f in files:
        mod = dtparse.isoparse(f["modifiedTime"])   # aware datetime UTC
        print(f"   • {f['name']:15}  mod={mod}  >? umbral={threshold}")
        if hours_window >= 876000 or mod > threshold:   # 876000 h ≈ 100 años para forzar full
            recent.append(f)
    return recent


def download_to_tmp(file_id, fname):
    buf = io.BytesIO()
    MediaIoBaseDownload(buf, drive.files().get_media(fileId=file_id)).next_chunk()
    buf.seek(0)
    tmp = NamedTemporaryFile(delete=False, suffix=".dbf"); tmp.write(buf.read()); tmp.close()
    return tmp.name


def sha1_dict(d):
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

# ─────────────────────────── MAIN ───────────────────────────────
print("⏳ Buscando archivos .DBF recientes…")
files = list_recent_dbf(HOURS_WINDOW)
print(f"🗂 A procesar: {len(files)} archivos (últimas {HOURS_WINDOW} h)\n")

for f in files:
    name = f["name"]; col_name = name.rsplit('.',1)[0].lower()
    print(f"📂 {name} → '{col_name}'")

    try:
        tmp = download_to_tmp(f["id"], name)
        table = DBF(tmp, load=True, ignore_missing_memofile=True, encoding=ENCODING)
        if len(table) == 0:
            print("⚠️  Vacío, omitido"); os.remove(tmp); continue

        key = table.field_names[0].lower(); col = db.collection(col_name)
        batch = db.batch(); cnt = 0; writes = skips = 0

        for rec in table:
            doc = {k.lower(): (str(v) if v is not None else None) for k, v in rec.items()}
            doc_id = str(doc[key]).strip()
            if not doc_id: continue
            doc["h"] = sha1_dict(doc)

            try:
                h_snap = col.document(doc_id).get(field_paths=["h"])
                if h_snap.exists and h_snap.get("h") == doc["h"]:
                    skips += 1; continue
            except Exception:
                pass

            batch.set(col.document(doc_id), doc)
            cnt += 1; writes += 1
            if cnt == BATCH_SIZE:
                safe_commit(batch); batch = db.batch(); cnt = 0; time.sleep(PAUSE_SEC)

        if cnt:
            safe_commit(batch)

        print(f"✅ escritos={writes}, saltados={skips}\n")
        os.remove(tmp)

    except ResourceExhausted:
        print("🚨 Cuota alcanzada, pausa 30 s"); time.sleep(30)
    except Exception as e:
        print(f"❌ Error en '{name}': {e}")

print("🎉 Sincronización COMPLETA")
