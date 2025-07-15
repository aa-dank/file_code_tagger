# sync_tables.py  –– psycopg v3, tqdm progress bar, dotenv creds
from psycopg import connect
from tqdm import tqdm
from dotenv import load_dotenv
import os

load_dotenv()

SRC_DSN = (
    f"postgresql://{os.getenv('APP_DB_USERNAME')}:{os.getenv('APP_DB_PASSWORD')}"
    f"@{os.getenv('APP_DB_HOST')}:{os.getenv('APP_DB_PORT')}/{os.getenv('APP_DB_NAME')}"
)
DST_DSN = (
    f"postgresql://{os.getenv('PROJECT_DB_USERNAME')}:{os.getenv('PROJECT_DB_PASSWORD')}"
    f"@{os.getenv('PROJECT_DB_HOST')}:{os.getenv('PROJECT_DB_PORT')}/{os.getenv('PROJECT_DB_NAME')}"
)

BATCH = 10_000

TABLES = [
    (
        "files",
        # ── hash is the business key ──────────────────────────────
        """INSERT INTO files (id, size, hash, extension)
           VALUES (%(id)s, %(size)s, %(hash)s, %(extension)s)
           ON CONFLICT (hash) DO UPDATE
             SET id        = EXCLUDED.id,      -- refresh surrogate
                 size      = EXCLUDED.size,
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

# ──────────────────────────────────────────────────────────────────────────────
def stream_and_upsert(src_cur, dst_cur, table, upsert_sql):
    src_cur.execute(f"SELECT COUNT(*) FROM {table}")
    total = src_cur.fetchone()[0]
    bar   = tqdm(total=total, desc=f"Sync {table}")

    src_cur = src_cur.connection.cursor(name=f"stream_{table}")
    src_cur.execute(f"SELECT * FROM {table} ORDER BY id")

    cols = [c.name for c in src_cur.description]

    while rows := src_cur.fetchmany(BATCH):
        dict_rows = [dict(zip(cols, r)) for r in rows]
        dst_cur.executemany(upsert_sql, dict_rows)
        bar.update(len(rows))

        # after each batch of *files* refresh hash in child tables
        if table == "files":
            dst_cur.execute(
                """
                UPDATE file_embeddings fe
                  SET file_hash = f.hash
                  FROM files f
                 WHERE fe.file_id  = f.id
                   AND fe.file_hash IS NULL;
                UPDATE file_tag_labels tl
                  SET file_hash = f.hash
                  FROM files f
                 WHERE tl.file_id = f.id
                   AND tl.file_hash IS NULL;
                """
            )

    bar.close()

# ──────────────────────────────────────────────────────────────────────────────
def main():
    with connect(SRC_DSN) as src_conn, connect(DST_DSN) as dst_conn:
        dst_conn.autocommit = False
        src_cur = src_conn.cursor()
        dst_cur = dst_conn.cursor()

        # order matters: files first (parent), then file_locations (child)
        for table, sql in TABLES:
            stream_and_upsert(src_cur, dst_cur, table, sql)
            dst_conn.commit()       # keep WAL small, visible progress

        src_cur.close()
        dst_cur.close()

if __name__ == "__main__":
    main()
