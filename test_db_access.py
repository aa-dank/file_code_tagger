import psycopg
from psycopg import sql
from psycopg.errors import OperationalError
import sys

from creds import APP_DB_USERNAME, APP_DB_PASSWORD, PROJECT_DB_USERNAME, PROJECT_DB_PASSWORD

app_db_dict = dict(
    host="128.114.128.27",
    port=5432,
    dbname="archives",
    user=APP_DB_USERNAME,
    password=APP_DB_PASSWORD,
)

project_db_dict = dict(
    host="localhost",
    port=5433,
    dbname="archives",
    user=PROJECT_DB_USERNAME,
    password=PROJECT_DB_PASSWORD,
)

def test_connection_and_info(cfg: dict, role: str) -> bool:
    """Test connection and print database information."""
    print(f"\n{'='*60}")
    print(f"Testing {role} Database")
    print(f"{'='*60}")
    print(f"Host: {cfg['host']}:{cfg['port']}")
    print(f"Database: {cfg['dbname']}")
    print(f"User: {cfg['user']}")
    
    try:
        with psycopg.connect(**cfg) as conn:
            with conn.cursor() as cur:
                # Basic connection test
                cur.execute("SELECT 1;")
                print("✓ Connection successful")
                
                # Database version
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                print(f"✓ PostgreSQL Version: {version}")
                
                # Current database size
                cur.execute("""
                    SELECT pg_size_pretty(pg_database_size(current_database()));
                """)
                db_size = cur.fetchone()[0]
                print(f"✓ Database Size: {db_size}")
                
                # List tables and their row counts
                cur.execute("""
                    SELECT schemaname, relname, n_tup_ins - n_tup_del as row_count
                    FROM pg_stat_user_tables 
                    ORDER BY schemaname, relname;
                """)
                tables = cur.fetchall()
                
                if tables:
                    print(f"✓ Tables ({len(tables)} found):")
                    for schema, table, rows in tables:
                        print(f"  • {schema}.{table}: {rows:,} rows")
                else:
                    print("✓ No user tables found")
                
                # Check for specific extensions (useful for pgvector)
                cur.execute("""
                    SELECT extname, extversion 
                    FROM pg_extension 
                    WHERE extname IN ('vector', 'pg_trgm', 'btree_gin', 'btree_gist')
                    ORDER BY extname;
                """)
                extensions = cur.fetchall()
                
                if extensions:
                    print("✓ Relevant Extensions:")
                    for ext_name, ext_version in extensions:
                        print(f"  • {ext_name} v{ext_version}")
                else:
                    print("✓ No relevant extensions found")
                    
    except OperationalError as err:
        print(f"✗ Connection failed: {err}")
        return False
    except Exception as err:
        print(f"✗ Error gathering database info: {err}")
        return False
    
    return True

def main():
    """Test both database connections and display information."""
    print("Database Access Test")
    print("=" * 60)
    
    success_count = 0
    
    # Test app database
    if test_connection_and_info(app_db_dict, "APP (Source)"):
        success_count += 1
    
    # Test project database  
    if test_connection_and_info(project_db_dict, "PROJECT (Target)"):
        success_count += 1
    
    print(f"\n{'='*60}")
    print(f"Summary: {success_count}/2 databases accessible")
    print(f"{'='*60}")
    
    if success_count < 2:
        sys.exit(1)

if __name__ == "__main__":
    main()