import sys
import os

venv_site_packages = os.path.abspath('kitchen_ops_app/.venv/lib/python3.14/site-packages')
sys.path.insert(0, venv_site_packages)
sys.path.insert(0, os.path.abspath('kitchen_ops_app'))

import psycopg2
import sqlite3
import app

def reset_and_migrate():
    print("Resetting DB...")
    pg_conn = psycopg2.connect(dbname='kitchen_ops', host='127.0.0.1')
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()
    pg_cur.execute("DROP SCHEMA IF EXISTS uga CASCADE")
    pg_cur.execute("DROP SCHEMA IF EXISTS mrra CASCADE")

    os.environ['DATABASE_URL'] = 'postgresql://localhost/kitchen_ops'
    app.init_db('uga')
    app.init_db('mrra')
    
    pg_conn.autocommit = False

    def migrate_db(sqlite_path, schema_name):
        print(f"Migrating {sqlite_path} to schema {schema_name}...")
        sl_conn = sqlite3.connect(sqlite_path)
        sl_cur = sl_conn.cursor()
        
        sl_cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in sl_cur.fetchall() if r[0] != 'sqlite_sequence']
        
        for table in tables:
            # Disable triggers to bypass foreign key checks temporarily during migration
            pg_cur.execute(f"ALTER TABLE {schema_name}.{table} DISABLE TRIGGER ALL")
            
            sl_cur.execute(f"SELECT * FROM {table}")
            rows = sl_cur.fetchall()
            if not rows: 
                pg_cur.execute(f"ALTER TABLE {schema_name}.{table} ENABLE TRIGGER ALL")
                continue
            
            cols = ', '.join([d[0] for d in sl_cur.description])
            vals = ', '.join(['%s'] * len(sl_cur.description))
            
            try:
                pg_cur.executemany(f"INSERT INTO {schema_name}.{table} ({cols}) VALUES ({vals}) ON CONFLICT DO NOTHING", rows)
                print(f"  Migrated {len(rows)} rows into {table}")
            except Exception as e:
                print(f"  Error on {table}: {e}")
                pg_conn.rollback()
                
            pg_cur.execute(f"ALTER TABLE {schema_name}.{table} ENABLE TRIGGER ALL")
            
        # Reset the SERIAL sequences so new inserts don't fail!
        for table in tables:
            try:
                pg_cur.execute(f"SELECT setval(pg_get_serial_sequence('{schema_name}.{table}', 'id'), COALESCE(MAX(id), 1) + 1, false) FROM {schema_name}.{table};")
            except Exception:
                pg_conn.rollback()
                
        sl_conn.close()

    migrate_db('kitchen_ops_app/kitchen_ops_uga.db', 'uga')
    migrate_db('kitchen_ops_app/kitchen_ops_mrra.db', 'mrra')
    
    pg_conn.commit()
    pg_conn.close()
    print("Migration complete!")

if __name__ == "__main__":
    reset_and_migrate()
