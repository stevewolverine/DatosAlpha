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
FOLDER_ID    = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"  # carpeta Drive
HOURS_WINDOW = 9999   # 1ª carga = grande; luego reduce, ej. 5
BATCH_SIZE   = 400      # ≤ 500 docs por commit
PAUSE_SEC    = 1        # pausa entre commits
ENCODING     = "latin1" # encoding archivos DBF
# ───────────────────────────

# Credenciales Google Drive
drive_creds = service_account.Credentials.from_service_account_info(
    json.loads(os.environ["DRIVE_KEY"]),
    scopes=["https://www.googleapis.com/auth/drive.readonly"],
)
drive = build("drive", "v3", credentials=drive_creds)

# Credenciales Firebase
firebase_creds = credentials.Certificate(json.loads(os.environ["FIREBASE_KEY"]))
firebase_admin.initialize_app(firebase_creds)
db = firestore.client()

# ------------------------------------------------------------------

def list_recent_dbf(hours_window):
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
    buf = io.BytesIO()
    MediaIoBaseDownload(buf, drive.files().get_media(fileId=file_id)).next_chunk()
    buf.seek(0)
    tmp = NamedTemporaryFile(delete=False, suffix=".dbf")
    tmp.write(buf.read()); tmp.close()
    return tmp.name


def sha1_dict(d):
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()


def safe_commit(batch, tries=3):
    for n in range(tries):
        try:
            batch.commit()
            return
        except Exception as e:
            wait = 5 * (n + 1)
            print(f"⏳ Commit falló ({n+1}/{tries}) → esperando {wait}s: {e}")
            time.sleep(wait)
    raise RuntimeError("Commit fallido tras reintentos")

# ------------------------------------------------------------------

print("⏳ Buscando archivos .DBF recientes…")
files = list_recent_dbf(HOURS_WINDOW)
print(f"🗂 {len(files)} archivos a procesar (últimas {HOURS_WINDOW} h)")

for f in files:
    name = f["name"]; col_name = name.rsplit('.',1)[0].lower()
    print(f"\n📂 {name} → colección '{col_name}'")

    try:
        tmp_path = download_to_tmp(f["id"], name)
        table = DBF(tmp_path, load=True, ignore_missing_memofile=True, encoding=ENCODING)
        if len(table) == 0:
            print("⚠️  Vacío, omitido."); os.remove(tmp_path); continue

        key_field = table.field_names[0].lower()
        col_ref   = db.collection(col_name)
        batch     = db.batch(); n_batch = 0; escritos = 0; omitidos = 0

        for rec in table:
            doc = {k.lower(): (str(v) if v is not None else None) for k, v in rec.items()}
            doc_id = str(doc[key_field]).strip()
            if not doc_id:
                continue

            doc_hash = sha1_dict(doc)
            doc["h"] = doc_hash

            try:
                snap = col_ref.document(doc_id).get(field_paths=["h"])
                if snap.exists and snap.get("h") == doc_hash:
                    omitidos += 1
                    continue
            except Exception:
                pass

            batch.set(col_ref.document(doc_id), doc)
            n_batch += 1; escritos += 1

            if n_batch == BATCH_SIZE:
                safe_commit(batch)
                batch = db.batch(); n_batch = 0
                time.sleep(PAUSE_SEC)

        if n_batch:
            safe_commit(batch)

        os.remove(tmp_path)
        print(f"✅ {escritos} escritos, {omitidos} sin cambios")

    except ResourceExhausted:
        print("🚨 Límite de escritura; pausa 30 s…"); time.sleep(30)
    except Exception as e:
        print(f"❌ Error procesando '{name}': {e}")

print("\n🎉 Sincronización COMPLETA")
