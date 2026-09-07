"""
Quick utility to query and inspect the SQLite database.
Usage: python query_db.py
"""
import sqlite3
import sys

DB_PATH = 'env_data.db'

def main():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        # List all tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cur.fetchall()]
        
        if not tables:
            print("No tables found. Run 'python db.py' first to initialize the schema.")
            return
        
        print(f"Database: {DB_PATH}")
        print(f"{'='*50}")
        
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM [{table}]")
            count = cur.fetchone()[0]
            print(f"  {table:30s} -> {count} rows")
        
        print(f"{'='*50}")
        
        # If a query was passed as argument, run it
        if len(sys.argv) > 1:
            query = ' '.join(sys.argv[1:])
            print(f"\nRunning: {query}")
            cur.execute(query)
            rows = cur.fetchall()
            if rows:
                # Print column names
                col_names = [desc[0] for desc in cur.description]
                print(' | '.join(col_names))
                print('-' * (len(' | '.join(col_names))))
                for row in rows:
                    print(' | '.join(str(v) for v in row))
            else:
                print("(no results)")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    main()
