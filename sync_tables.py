from psycopg import connect          # psycopg v3
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()

APP_DB_USERNAME = os.environ.get("APP_DB_USERNAME")
APP_DB_PASSWORD = os.environ.get("APP_DB_PASSWORD")
APP_DB_NAME = os.environ.get("APP_DB_NAME")
APP_DB_PORT = os.environ.get("APP_DB_PORT")
APP_DB_HOST = os.environ.get("APP_DB_HOST")

PROJECT_DB_USERNAME = os.environ.get("PROJECT_DB_USERNAME")
PROJECT_DB_PASSWORD = os.environ.get("PROJECT_DB_PASSWORD")
PROJECT_DB_NAME = os.environ.get("PROJECT_DB_NAME")
PROJECT_DB_PORT = os.environ.get("PROJECT_DB_PORT")
PROJECT_DB_HOST = os.environ.get("PROJECT_DB_HOST")

SRC_DSN = f"postgresql://{APP_DB_USERNAME}:{APP_DB_PASSWORD}@{APP_DB_HOST}:{APP_DB_PORT}/{APP_DB_NAME}"
DST_DSN = f"postgresql://{PROJECT_DB_USERNAME}:{PROJECT_DB_PASSWORD}@{PROJECT_DB_HOST}:{PROJECT_DB_PORT}/{PROJECT_DB_NAME}"

BATCH = 10_000         # rows per fetch/insert

TABLES = [
    (
        "files",
        """INSERT INTO files (id, size, hash, extension)
           VALUES (%(id)s, %(size)s, %(hash)s, %(extension)s)
           ON CONFLICT (id) DO UPDATE SET
              size = EXCLUDED.size,
              hash = EXCLUDED.hash,
              extension = EXCLUDED.extension"""
    ),
    (
        "file_locations",
        """INSERT INTO file_locations
             (id, file_id, existence_confirmed, hash_confirmed,
              file_server_directories, filename)
           VALUES (%(id)s, %(file_id)s, %(existence_confirmed)s,
                   %(hash_confirmed)s, %(file_server_directories)s,
                   %(filename)s)
           ON CONFLICT (id) DO UPDATE SET
              file_id              = EXCLUDED.file_id,
              existence_confirmed  = EXCLUDED.existence_confirmed,
              hash_confirmed       = EXCLUDED.hash_confirmed,
              file_server_directories = EXCLUDED.file_server_directories,
              filename             = EXCLUDED.filename"""
    ),
]

def stream_and_upsert(src_cur, dst_cur, table, upsert_sql):
    # get row count for a progress bar
    src_cur.execute(f"SELECT COUNT(*) FROM {table}")
    total = src_cur.fetchone()[0]
    bar = tqdm(total=total, desc=f"Sync {table}")

    # server-side cursor streams rows without big RAM hit
    src_cur = src_cur.connection.cursor(name=f"stream_{table}")

    src_cur.execute(f"SELECT * FROM {table} ORDER BY id")
    
    # Get column names from the cursor description
    columns = [col.name for col in src_cur.description]
    
    while True:
        rows = src_cur.fetchmany(BATCH)
        if not rows:
            break

        # Convert tuples to dictionaries using column names
        dict_rows = [dict(zip(columns, row)) for row in rows]
        dst_cur.executemany(upsert_sql, dict_rows)
        bar.update(len(rows))

    bar.close()

def main():
    with connect(SRC_DSN) as src_conn, connect(DST_DSN) as dst_conn:
        # autocommit off → commit after each table
        dst_conn.autocommit = False

        src_cur = src_conn.cursor()   # we’ll open named cursors inside loop
        dst_cur = dst_conn.cursor()

        # copy files first (file_locations has FK)
        for table, upsert_sql in TABLES:
            stream_and_upsert(src_cur, dst_cur, table, upsert_sql)
            dst_conn.commit()         # keeps WAL segments small

        src_cur.close()
        dst_cur.close()

if __name__ == "__main__":
    main()