import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()

DB_ENGINE = os.getenv('DB_ENGINE', 'sqlite').lower()
SQLITE_DB_PATH = os.getenv('SQLITE_DB_PATH', 'env_data.db')

def get_db_connection():
    """
    Returns a database connection. Defaults to SQLite for local prototyping.
    Can be switched to PostgreSQL when DB_ENGINE=postgres.
    """
    if DB_ENGINE == 'postgres':
        import psycopg2
        return psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            dbname=os.getenv('DB_NAME', 'env_data'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'your_password')
        )
    else:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

def execute_query(cursor, query, params=()):
    """
    Executes query handling placeholder translation between SQLite (?) and Postgres (%s).
    """
    if DB_ENGINE != 'postgres':
        query = query.replace('%s', '?')
    cursor.execute(query, params)

def init_db(schema_file='schema.sql'):
    """Applies schema.sql to the database."""
    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"{schema_file} not found.")
        
    with open(schema_file, 'r') as f:
        schema_sql = f.read()
        
    conn = get_db_connection()
    try:
        if DB_ENGINE == 'postgres':
            cursor = conn.cursor()
            cursor.execute(schema_sql)
            conn.commit()
        else:
            conn.executescript(schema_sql)
            conn.commit()
        print(f"Database initialized successfully using engine: {DB_ENGINE}")
    finally:
        conn.close()

if __name__ == '__main__':
    init_db()
