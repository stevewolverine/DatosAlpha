#!/usr/bin/env python3
"""
Sincroniza .DBF (Google Drive) → Firebase Firestore
- Sube solo archivos modificados en las últimas HOURS_WINDOW horas
- Para cada registro:
    • Usa la columna clave definida en KEY_FIELD como ID de documento
    • Solo procesa si pertenece al CURRENT_YEAR
      · Para tablas con fecha propia → DATE_FIELD
      · Para tablas detalle        → RELATED_DATE (usa fecha del encabezado)
    • Solo escribe si el hash SHA-1 cambia
Requiere:
  pip install dbfread google-api-python-client google-auth google-auth-httplib2 \
              google-auth-oauthlib firebase-admin python-dateutil
Entorno:
  DRIVE_KEY    → JSON cred. servicio Drive
  FIREBASE_KEY → JSON cred. servicio Firebase
"""

import os, io, json, time, hashlib
from datetime import datetime, timedelta, timezone
from tempfile import NamedTemporaryFile
from dateutil import parser as dtparse
from collections import defaultdict

from dbfread import DBF
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.api_core.exceptions import ResourceExhausted
import firebase_admin
from firebase_admin import credentials, firestore

# ───────── AJUSTES GENERALES ─────────
FOLDER_ID     = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"
HOURS_WINDOW  = 5               # solo archivos recientes
BATCH_SIZE    = 400               # ≤500
PAUSE_SEC     = 1                 # entre commits
ENCODING      = "latin1"
CURRENT_YEAR  = 2025              # ← año a conservar
# ─────────────────────────────────────

# ── Clave primaria por tabla (.dbf → campo) ──
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

# ── Campo-fecha propio por tabla ──
DATE_FIELD = {
    "facturac" : "FALTA_FAC",   # dd/mm/aaaa
    "creditos" : "FECHA",
    # Otras tablas con fecha propia aquí…
}

# ── Tablas detalle que dependen de la fecha de un encabezado ──
#  detalle → (tabla_encabezado, campo_clave_enc, campo_fecha_enc)
RELATED_DATE = {
    "factentr"  : ("facturac", "NO_FAC", "FALTA_FAC"),
    "facturad"  : ("facturac", "NO_FAC", "FALTA_FAC"),
    "creditod"  : ("creditos", "NO_NOTA", "FECHA"),
}

# ───────── CONEXIONES ─────────
drive_creds = service_account.Credentials.from_service_account_info(
    json.loads(os.environ["DRIVE_KEY"]),
    scopes=["https://www.googleapis.com/auth/drive.readonly"],
)
drive = build("drive", "v3", credentials=drive_creds)

fb_creds = credentials.Certificate(json.loads(os.environ["FIREBASE_KEY"]))
firebase_admin.initialize_app(fb_creds)
db = firestore.client()

# ───────── UTILIDADES ─────────
def collection_exists(col_id: str) -> bool:
    # Devuelve True si la colección ya tiene al menos 1 doc
    try:
        next(db.collection(col_id).limit(1).stream())
        return True
    except StopIteration:
        return False

def safe_commit(batch, retries=3):
    for n in range(retries):
        try:
            batch.commit(); return
        except Exception as e:
            wait = 5 * (n + 1)
            print(f"⏳ Commit falló ({n+1}/{retries}) → {wait}s: {e}")
            time.sleep(wait)
    raise RuntimeError("Commit fallido tras reintentos")

def list_recent_dbf():
    th = datetime.now(timezone.utc) - timedelta(hours=HOURS_WINDOW)
    q  = (f"'{FOLDER_ID}' in parents and "
          "mimeType!='application/vnd.google-apps.folder' and name contains '.DBF'")
    files = drive.files().list(q=q, fields="files(id,name,modifiedTime)").execute()["files"]

    selected = []
    for f in files:
        name = f["name"].lower()
        col  = name.rsplit('.',1)[0]
        rec  = dtparse.isoparse(f["modifiedTime"])
        # ① dentro de la ventana  OR  ② la colección no existe aún
        if rec > th or not collection_exists(col):
            selected.append(f)
    return selected


def download_tmp(file_id):
    buf = io.BytesIO()
    MediaIoBaseDownload(buf, drive.files().get_media(fileId=file_id)).next_chunk()
    tmp = NamedTemporaryFile(delete=False, suffix=".dbf")
    tmp.write(buf.getvalue()); tmp.close()
    return tmp.name

def sha1_dict(d): return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

def extract_year(value):
    if value is None or str(value).strip() == "":
        return None
    if hasattr(value, "year"):
        return value.year
    try:
        return dtparse.parse(str(value)).year
    except Exception:
        return None

# ───────── PRE-CARGA AÑOS DE ENCABEZADO ─────────
print("⏳ Buscando archivos recientes…")
files = list_recent_dbf()
file_map = {f["name"].lower(): f for f in files}

header_year: dict[str, dict[str,int]] = defaultdict(dict)  # tabla → {doc_id: año}

for det, (hdr_tab, hdr_key, hdr_date) in RELATED_DATE.items():
    hdr_name = f"{hdr_tab}.dbf"
    hdr_file = file_map.get(hdr_name)
    if not hdr_file:
        print(f"⚠️  Encabezado {hdr_name} no está en la ventana, se omitirá filtro de año")
        continue

    path = download_tmp(hdr_file["id"])
    hdr_dbf = DBF(path, load=True, ignore_missing_memofile=True, encoding=ENCODING)
    for rec in hdr_dbf:
        doc_id = str(rec[hdr_key]).strip()
        yr = extract_year(rec[hdr_date])
        if doc_id and yr is not None:
            header_year[hdr_tab][doc_id] = yr
    os.remove(path)
    print(f"📑 Cacheado año de {len(header_year[hdr_tab])} registros de {hdr_tab}")

print(f"\n🗂 Archivos a procesar (últimas {HOURS_WINDOW} h): {len(files)}\n")

# ───────── PROCESA CADA .DBF ─────────
for f in files:
    name = f["name"]; col_name = name.rsplit('.',1)[0].lower()
    print(f"📂 {name} → colección '{col_name}'")

    try:
        tmp = download_tmp(f["id"])
        table = DBF(tmp, load=True, ignore_missing_memofile=True, encoding=ENCODING)
        if len(table) == 0:
            print("⚠️  Vacío, omitido"); os.remove(tmp); continue

        key_field = KEY_FIELD.get(col_name, table.field_names[0]).upper()
        date_field = DATE_FIELD.get(col_name)

        # Info de tabla relacionada (si aplica)
        rel_info = RELATED_DATE.get(col_name)
        col_fb = db.collection(col_name)
        batch = db.batch(); cnt = writes = skips = 0

        for rec in table:
            # ── FILTRO POR AÑO ───────────────────────────
            yr = None
            if date_field:
                yr = extract_year(rec[date_field])
            elif rel_info:
                hdr_tab, hdr_key, _ = rel_info
                rel_id = str(rec[hdr_key]).strip()
                yr = header_year.get(hdr_tab, {}).get(rel_id)

            # Solo se descarta si SÍ conocemos el año y es distinto al actual
            if yr is not None and yr != CURRENT_YEAR:
                continue
            # ─────────────────────────────────────────────

            doc = {k.lower(): (str(v) if v is not None else None) for k, v in rec.items()}
            doc_id = str(rec[key_field]).strip()
            if not doc_id:
                continue
            doc["h"] = sha1_dict(doc)

            try:
                snap = col_fb.document(doc_id).get(field_paths=["h"])
                if snap.exists and snap.get("h") == doc["h"]:
                    skips += 1
                    continue
            except Exception:
                pass

            batch.set(col_fb.document(doc_id), doc)
            cnt += 1
            writes += 1
            if cnt == BATCH_SIZE:
                safe_commit(batch)
                batch = db.batch()
                cnt = 0
                time.sleep(PAUSE_SEC)

        if cnt:
            safe_commit(batch)

        print(f"✅ escritos={writes}, saltados={skips}\n")
        os.remove(tmp)

    except ResourceExhausted:
        print("🚨 Cuota Firestore alcanzada, pausa 30 s"); time.sleep(30)
    except Exception as e:
        print(f"❌ Error en '{name}': {e}")

print("🎉 Sincronización COMPLETA")

