# etl.py  –  Descarga .DBF de Google Drive y los carga en Supabase
# Requiere: pip install dbfread psycopg[binary] google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib

import os
import json          
import psycopg
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dbfread import DBF
import psycopg
import io
import csv

# ------- Credenciales Google Drive (se inyectan como SECRET) --------
drive_creds = service_account.Credentials.from_service_account_info(
    json.loads(os.environ["DRIVE_KEY"]),
    scopes=["https://www.googleapis.com/auth/drive.readonly"],
)
drive = build("drive", "v3", credentials=drive_creds)

FOLDER_ID = "1kgnfsfNnkxxC8o-BfBx_fssv751tLNzL"    # <-- ID de la carpeta en Drive con los .DBF

# ------- Conexión Supabase (se inyecta como SECRET) -----------------
dsn  = os.environ["SUPA_DSN"]
conn = psycopg.connect(dsn, connect_timeout=10)
cur = conn.cursor()

def list_dbf_files():
    q = f"'{FOLDER_ID}' in parents and mimeType != 'application/vnd.google-apps.folder'"
    files = drive.files().list(q=q, fields="files(id,name)").execute()["files"]
    return [f for f in files if f["name"].lower().endswith(".dbf")]

def download_file(file_id):
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = drive._http.request
    status, body = downloader(request.uri)
    buf.write(body)
    buf.seek(0)
    return buf

def load_dbf_to_pg(buf, table_name):
    dbf = DBF(buf, load=True)             # dbfread acepta BytesIO
    cols = [f.name.lower() for f in dbf.fields]

    # Crea tabla si no existe (todas columnas texto; ajusta si requieres tipos)
    col_def = ", ".join(f'"{c}" text' for c in cols)
    cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_def});')

    # Borra datos previos (refresh completo).  Usa UPSERT si quieres incremental
    cur.execute(f'TRUNCATE "{table_name}";')

    # COPY vía buffer TSV
    copy_buf = io.StringIO()
    writer = csv.writer(copy_buf, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    for rec in dbf:
        writer.writerow([(rec[c.upper()] or r"\N") for c in cols])
    copy_buf.seek(0)

    cur.copy(
        f'COPY "{table_name}" FROM STDIN WITH (FORMAT csv, DELIMITER E\'\\t\', NULL \'\\N\')',
        copy_buf,
    )
    conn.commit()

def debug_tables():
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
    print("Tablas en Supabase (esquema public):")
    for row in cur.fetchall():
        print(" -", row[0])

if __name__ == "__main__":
    main()
