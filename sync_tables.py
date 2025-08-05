# sync_tables.py
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
             SET size      = EXCLUDED.size,
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

    # For file_locations, pre-fetch all valid file IDs from destination
    # Valid file IDs are those that exist in the files table
    valid_file_ids = set()
    if table == "file_locations":
        dst_cur.execute("SELECT id FROM files")
        valid_file_ids = {row[0] for row in dst_cur.fetchall()}
        print(f"\nFound {len(valid_file_ids)} valid file IDs in destination database")

    while rows := src_cur.fetchmany(BATCH):
        dict_rows = [dict(zip(cols, r)) for r in rows]
        
        # Filter out file_locations with invalid file_id references
        if table == "file_locations":
            original_count = len(dict_rows)
            dict_rows = [row for row in dict_rows if row['file_id'] in valid_file_ids]
            skipped = original_count - len(dict_rows)
            if skipped > 0:
                print(f"Skipped {skipped} file_locations with invalid file_id references")
        
        if dict_rows:  # Only execute if we have rows to process
            dst_cur.executemany(upsert_sql, dict_rows)
        
        bar.update(len(rows))  # Update progress bar based on source rows processed

        # after each batch of *files* refresh hash in child tables
        if table == "files":
            dst_cur.execute(
                """
                UPDATE file_embeddings fe
                  SET file_hash = f.hash
                  FROM files f
                 WHERE fe.file_hash = f.hash
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

