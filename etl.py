# etl.py  –  Drive ➜ Supabase
import io, json, ssl, csv, psycopg, pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dbfread import DBF

# --- Drive auth ----------------------------------------------------
creds = service_account.Credentials.from_service_account_info(
    json.loads(open('drive_key.json').read()),
    scopes=['https://www.googleapis.com/auth/drive.readonly']
)
drive = build('drive', 'v3', credentials=creds)

FOLDER_ID = '1AbCDEFghijk...'   # carpeta de los DBF

def list_dbf_files():
    q = f"'{FOLDER_ID}' in parents and name contains '.dbf'"
    return drive.files().list(q=q, fields="files(id,name)").execute()['files']

def download_file(file_id):
    buf = io.BytesIO()
    request = drive.files().get_media(fileId=file_id)
    downloader = build('drive', 'v3', credentials=creds).files().get_media(fileId=file_id)
    downloader.execute(buf)
    buf.seek(0)
    return buf

# --- Supabase connection -------------------------------------------
dsn = f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASS']}@" \
      f"{os.environ['DB_HOST']}:5432/{os.environ['DB_NAME']}?sslmode=require"
conn = psycopg.connect(dsn)
cur  = conn.cursor()

def load_dbf(buf, table):
    dbf = DBF(buf, load=True)        # dbfread acepta file-like obj
    cols = [f.name.lower() for f in dbf.fields]
    cur.execute(f'create table if not exists "{table}" ({", ".join(c+" text" for c in cols)});')

    copy_buf = io.StringIO()
    w = csv.writer(copy_buf, delimiter='\t', lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    for rec in dbf: w.writerow([rec[c.upper()] or r'\N' for c in cols])
    copy_buf.seek(0)

    cur.execute(f'truncate "{table}"')     # full refresh; usa upsert si prefieres
    cur.copy(f'COPY "{table}" FROM STDIN WITH (FORMAT csv, DELIMITER E\'\\t\', NULL \'\\N\')', copy_buf)
    conn.commit()

for f in list_dbf_files():
    print("Synching", f['name'])
    load_dbf(download_file(f['id']), f['name'].rsplit('.',1)[0].lower())

cur.close(); conn.close()
