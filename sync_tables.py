#!/usr/bin/env python3

import psycopg
from psycopg import sql
from psycopg.errors import OperationalError
import sys
from datetime import datetime
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

from creds import APP_DB_USERNAME, APP_DB_PASSWORD, PROJECT_DB_USERNAME, PROJECT_DB_PASSWORD
from db_models import Base, File, FileLocation

# Source database configuration (app database)
SRC_CONFIG = {
    "host": "128.114.128.27",
    "port": 5432,
    "dbname": "archives",
    "user": APP_DB_USERNAME,
    "password": APP_DB_PASSWORD,
}

# Target database configuration (project database)
DST_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "archives",
    "user": PROJECT_DB_USERNAME,
    "password": PROJECT_DB_PASSWORD,
}

TABLES_TO_SYNC = ['files', 'file_locations']

def create_sqlalchemy_engine(config):
    """Create SQLAlchemy engine from psycopg config."""
    connection_string = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
    return create_engine(connection_string)

def ensure_tables_exist(dst_engine):
    """Ensure target tables exist, create them if they don't."""
    print("→ Ensuring target tables exist...")
    
    # Create only the tables we need
    tables_to_create = [Base.metadata.tables['files'], Base.metadata.tables['file_locations']]
    
    for table in tables_to_create:
        table.create(dst_engine, checkfirst=True)
        print(f"  ✓ Table '{table.name}' ready")

def get_table_row_count(conn, table_name):
    """Get row count for a table."""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
        return cur.fetchone()[0]

def truncate_tables(dst_conn):
    """Truncate target tables in correct order (respecting foreign keys)."""
    print("→ Truncating target tables...")
    
    with dst_conn.cursor() as cur:
        # Truncate in reverse dependency order
        cur.execute("TRUNCATE file_locations, files RESTART IDENTITY CASCADE;")
        dst_conn.commit()
        print("  ✓ Tables truncated")

def copy_table_data(src_conn, dst_conn, table_name):
    """Copy all data from source table to destination table."""
    print(f"→ Copying {table_name} data...")
    
    # Get source data
    with src_conn.cursor() as src_cur:
        src_cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name)))
        
        if table_name == 'files':
            columns = ['id', 'size', 'hash', 'extension']
        elif table_name == 'file_locations':
            columns = ['id', 'file_id', 'existence_confirmed', 'hash_confirmed', 'file_server_directories', 'filename']
        
        rows = src_cur.fetchall()
        
        if not rows:
            print(f"  ✓ No data to copy for {table_name}")
            return 0
        
        # Insert into destination
        with dst_conn.cursor() as dst_cur:
            column_names = sql.SQL(', ').join(map(sql.Identifier, columns))
            placeholders = sql.SQL(', ').join(sql.Placeholder() * len(columns))
            
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                column_names,
                placeholders
            )
            
            dst_cur.executemany(insert_query, rows)
            dst_conn.commit()
            
            print(f"  ✓ Copied {len(rows)} rows to {table_name}")
            return len(rows)

def sync_tables():
    """Main sync function."""
    print("Database Table Sync")
    print("=" * 60)
    print(f"Source: {SRC_CONFIG['host']}:{SRC_CONFIG['port']}/{SRC_CONFIG['dbname']}")
    print(f"Target: {DST_CONFIG['host']}:{DST_CONFIG['port']}/{DST_CONFIG['dbname']}")
    print(f"Tables: {', '.join(TABLES_TO_SYNC)}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # Create SQLAlchemy engine for table creation
        dst_engine = create_sqlalchemy_engine(DST_CONFIG)
        
        # Ensure tables exist
        ensure_tables_exist(dst_engine)
        
        # Connect to both databases
        with psycopg.connect(**SRC_CONFIG) as src_conn, \
             psycopg.connect(**DST_CONFIG) as dst_conn:
            
            print("✓ Connected to both databases")
            
            # Show source table counts
            print("\n→ Source table counts:")
            for table in TABLES_TO_SYNC:
                count = get_table_row_count(src_conn, table)
                print(f"  • {table}: {count:,} rows")
            
            # Truncate target tables
            truncate_tables(dst_conn)
            
            # Copy data table by table (in dependency order)
            total_rows = 0
            
            # Copy files first (no dependencies)
            rows_copied = copy_table_data(src_conn, dst_conn, 'files')
            total_rows += rows_copied
            
            # Copy file_locations (depends on files)
            rows_copied = copy_table_data(src_conn, dst_conn, 'file_locations')
            total_rows += rows_copied
            
            # Show final counts
            print("\n→ Target table counts:")
            for table in TABLES_TO_SYNC:
                count = get_table_row_count(dst_conn, table)
                print(f"  • {table}: {count:,} rows")
            
            print("\n" + "=" * 60)
            print(f"✓ Sync complete - {total_rows:,} total rows copied")
            print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            
    except OperationalError as err:
        print(f"✗ Database connection error: {err}")
        sys.exit(1)
    except Exception as err:
        print(f"✗ Sync failed: {err}")
        sys.exit(1)

if __name__ == "__main__":
    sync_tables()
