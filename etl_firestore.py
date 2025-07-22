#!/usr/bin/env python3
"""
Sincroniza archivos .DBF (Google Drive) → Firebase Firestore

• Procesa solo *.dbf* (ignora cualquier otro archivo en la carpeta)
• Sube únicamente archivos recientes (HOURS_WINDOW) salvo que la colección aún no exista
• Crea/actualiza documentos por clave primaria; añade hash 'h' para control incremental
• Para FACTURAD y CREDITOD genera IDs compuestos:  NO_FAC_001 ,  NO_NOTA_001 …
• Agrupa escrituras en lotes de 400 con reintentos exponenciales

Requiere:
  pip install dbfread google-api-python-client google-auth google-auth-httplib2 \
              google-auth-oauthlib firebase-admin python-dateutil
Variables de entorno:
  DRIVE_KEY   – JSON credencial de servicio Drive
  FIREBASE_KEY – JSON credencial de servicio Firebase
"""

import os
import io
import json
import time
import hashlib
from datetime import datetime, timedelta, timezone
from tempfile import NamedTemporaryFile
from collections import defaultdict
from typing import Dict, List, Any, Optional

from dateutil import parser as dtparse
from dbfread import DBF, DBFNotFound
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.api_core.exceptions import GoogleAPICallError, ResourceExhausted
import firebase_admin
from firebase_admin import credentials, firestore

# ───────── AJUSTES GENERALES ─────────
FOLDER_ID = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"
HOURS_WINDOW = 5
BATCH_SIZE = 400
PAUSE_SEC = 1
ENCODING = "latin1"
## REF: Obtener el año actual dinámicamente para evitar hardcoding.
CURRENT_YEAR = datetime.now().year

# ───────── CONFIGURACIÓN DE TABLAS ─────────
KEY_FIELD = {
    "producto": "CVE_PROD", "clientes": "CVE_CTE", "creditod": "NO_NOTA",
    "creditos": "NO_NOTA", "existe": "CVE_PROD", "factentr": "NO_FAC",
    "facturac": "NO_FAC", "facturad": "NO_FAC", "precioprod": "CVE_PROD",
    "prod_desc": "CVE_PROD", "prodimag": "CVE_PROD",
}
DATE_FIELD = {"facturac": "FALTA_FAC", "creditos": "FECHA"}
RELATED_DATE = {
    "factentr": ("facturac", "NO_FAC", "FALTA_FAC"),
    "facturad": ("facturac", "NO_FAC", "FALTA_FAC"),
    "creditod": ("creditos", "NO_NOTA", "FECHA"),
}

# ───────── CONEXIONES ─────────
try:
    drive_creds = service_account.Credentials.from_service_account_info(
        json.loads(os.environ["DRIVE_KEY"]),
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    drive = build("drive", "v3", credentials=drive_creds)

    fb_creds = credentials.Certificate(json.loads(os.environ["FIREBASE_KEY"]))
    firebase_admin.initialize_app(fb_creds)
    db = firestore.client()
except KeyError as e:
    raise SystemExit(f"❌ Error: Variable de entorno no encontrada: {e}")
except (ValueError, json.JSONDecodeError) as e:
    raise SystemExit(f"❌ Error: El JSON de la credencial es inválido: {e}")


# ───────── UTILIDADES ─────────
_collection_exists_cache = {}
def collection_exists(col_id: str) -> bool:
    ## REF: Cachear resultado para evitar llamadas repetidas a Firestore.
    if col_id in _collection_exists_cache:
        return _collection_exists_cache[col_id]
    try:
        next(db.collection(col_id).limit(1).stream())
        _collection_exists_cache[col_id] = True
        return True
    except StopIteration:
        _collection_exists_cache[col_id] = False
        return False

def safe_commit(batch, retries=3):
    for n in range(retries):
        try:
            batch.commit()
            return
        ## REF: Capturar excepciones más específicas.
        except (GoogleAPICallError, ResourceExhausted) as e:
            wait = 5 * (n + 1)
            print(f"⏳ Commit falló ({n+1}/{retries}), reintentando en {wait}s: {e}")
            time.sleep(wait)
    raise RuntimeError("Commit fallido tras reintentos")

def list_recent_dbf() -> List[Dict[str, Any]]:
    threshold = datetime.now(timezone.utc) - timedelta(hours=HOURS_WINDOW)
    query = f"'{FOLDER_ID}' in parents and mimeType!='application/vnd.google-apps.folder'"
    files = drive.files().list(q=query, fields="files(id,name,modifiedTime)").execute().get("files", [])
    
    selected_files = []
    for f in files:
        if not f["name"].lower().endswith(".dbf"):
            continue
        
        collection_name = f["name"].rsplit(".", 1)[0].lower()
        modified_time = dtparse.isoparse(f["modifiedTime"])
        
        if modified_time > threshold or not collection_exists(collection_name):
            selected_files.append(f)
            
    return selected_files

## REF: Usar un bloque 'with' para garantizar que el archivo temporal se elimine.
def download_tmp(file_id: str) -> NamedTemporaryFile:
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        _, done = downloader.next_chunk()
        
    fh.seek(0)
    tmp_file = NamedTemporaryFile(delete=False, suffix=".dbf")
    tmp_file.write(fh.getvalue())
    tmp_file.close()
    return tmp_file.name

def sha1_dict(d: Dict) -> str:
    return hashlib.sha1(json.dumps(d, sort_keys=True, default=str).encode()).hexdigest()

def extract_year(val: Any) -> Optional[int]:
    if val is None or str(val).strip() == "":
        return None
    if isinstance(val, (datetime, datetime.date)):
        return val.year
    try:
        ## REF: Capturar excepciones específicas.
        return dtparse.parse(str(val)).year
    except (ValueError, dtparse.ParserError):
        return None

# ── PROCESAMIENTO ──
def preprocess_header_dates(files_to_process: List[Dict], file_map: Dict) -> Dict:
    """ ## REF: Nueva función para cargar fechas de encabezados solo si es necesario. """
    header_year_map = defaultdict(dict)
    for detail_table, (header_table, key, date_field) in RELATED_DATE.items():
        header_filename = f"{header_table}.dbf"
        # Solo procesar si el archivo de encabezado está en la lista de modificados.
        if header_filename in file_map:
            print(f"  Pre-cargando fechas de '{header_filename}'...")
            header_file_info = file_map[header_filename]
            tmp_path = download_tmp(header_file_info["id"])
            try:
                for rec in DBF(tmp_path, encoding=ENCODING, ignore_missing_memofile=True):
                    doc_id = str(rec.get(key, "")).strip()
                    year = extract_year(rec.get(date_field))
                    if doc_id and year is not None:
                        header_year_map[header_table][doc_id] = year
            finally:
                os.remove(tmp_path)
    return header_year_map

def process_file(file_info: Dict, header_year_map: Dict):
    """ ## REF: Función principal para procesar un solo archivo DBF. """
    name = file_info["name"]
    col_name = name.rsplit(".", 1)[0].lower()
    print(f"\n📂 {name} → colección '{col_name}'")

    tmp_path = download_tmp(file_info["id"])
    try:
        table = DBF(tmp_path, load=True, ignore_missing_memofile=True, encoding=ENCODING)
        if not table:
            print("  ⚠️ Vacío, omitido."); return

        key_field = KEY_FIELD.get(col_name, table.field_names[0]).upper()
        
        col_ref = db.collection(col_name)
        batch = db.batch()
        writes = skips = cnt = 0
        line_counters = defaultdict(int)

        # Lógica especial para 'existe'
        records_iterator = table
        if col_name == "existe":
            records_iterator = [
                rec for rec in table 
                if str(rec.get("LUGAR", "")).strip().upper() == "LINEA"
            ]

        for rec in records_iterator:
            # Filtro por año
            year = None
            if col_name in DATE_FIELD:
                year = extract_year(rec.get(DATE_FIELD[col_name]))
            elif col_name in RELATED_DATE:
                hdr_tab, hdr_key, _ = RELATED_DATE[col_name]
                key_val = str(rec.get(hdr_key, "")).strip()
                year = header_year_map.get(hdr_tab, {}).get(key_val)

            if year is not None and year != CURRENT_YEAR:
                continue

            # ID del documento
            key_val = str(rec.get(key_field, "")).strip()
            if not key_val:
                continue

            doc_id = key_val
            if col_name in ("facturad", "creditod"):
                line_counters[key_val] += 1
                doc_id = f"{key_val}_{line_counters[key_val]:03d}"
                rec["ROW_ID"] = line_counters[key_val]

            doc_ref = col_ref.document(doc_id)
            doc_data = {k.lower(): v for k, v in rec.items()}
            # Convertir todos los valores a string para consistencia, manejando None
            doc_data = {k: str(v) if v is not None else None for k, v in doc_data.items()}
            doc_data["h"] = sha1_dict(doc_data)

            try: # Comprobar hash para evitar escrituras innecesarias
                if doc_ref.get(field_paths=["h"]).get("h") == doc_data["h"]:
                    skips += 1
                    continue
            except GoogleAPICallError: # El documento probablemente no existe
                pass
            
            batch.set(doc_ref, doc_data, merge=True)
            cnt += 1
            writes += 1
            if cnt >= BATCH_SIZE:
                safe_commit(batch)
                batch = db.batch()
                cnt = 0
                time.sleep(PAUSE_SEC)

        if cnt > 0:
            safe_commit(batch)
        print(f"  ✅ Escritos={writes}, Saltados={skips}")

    except DBFNotFound:
        print(f"  ❌ Error: No se pudo encontrar el archivo DBF en la ruta: {tmp_path}")
    except Exception as e:
        print(f"  ❌ Error inesperado en '{name}': {type(e).__name__} {e}")
    finally:
        os.remove(tmp_path)


def main():
    """Flujo principal del script."""
    print("⏳ Buscando archivos recientes...")
    files_to_process = list_recent_dbf()

    if not files_to_process:
        print("🎉 No hay archivos nuevos para sincronizar. Finalizado.")
        return

    print(f"🗂 {len(files_to_process)} archivos a procesar:")
    for f in files_to_process: print(f"  - {f['name']}")

    file_map = {f["name"].lower(): f for f in files_to_process}
    
    header_year_map = preprocess_header_dates(files_to_process, file_map)
    
    for f in files_to_process:
        try:
            process_file(f, header_year_map)
        except ResourceExhausted:
            print("🚨 Cuota de Firestore alcanzada, pausando 30s...")
            time.sleep(30)
        except Exception as e:
            print(f"🚨 Fallo crítico procesando {f['name']}: {e}")

    print("\n🎉 Sincronización COMPLETA")


if __name__ == "__main__":
    main()

