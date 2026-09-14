import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()

# BUGFIX: schema.sql and env_data.db were resolved against the *caller's* cwd.
# The scheduled task launches scheduler.py from a different working directory,
# so init_db() raised "schema.sql not found." on every job and pipeline_run_log
# never existed. Anchor both to this module's directory instead, the way
# scheduler.py and storage.py already do.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _resolve(path):
    """Absolute paths pass through; relative ones anchor to the repo root."""
    return path if os.path.isabs(path) else os.path.join(BASE_DIR, path)

DB_ENGINE = os.getenv('DB_ENGINE', 'sqlite').lower()
SQLITE_DB_PATH = _resolve(os.getenv('SQLITE_DB_PATH', 'env_data.db'))

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
    Executes query handling placeholder translation between SQLite (?) and Postgres (%s),
    as well as conflict handling (INSERT OR IGNORE vs ON CONFLICT DO NOTHING).
    """
    if DB_ENGINE == 'postgres':
        if 'INSERT OR IGNORE INTO' in query:
            query = query.replace('INSERT OR IGNORE INTO', 'INSERT INTO')
            if 'ON CONFLICT' not in query:
                query = query + ' ON CONFLICT DO NOTHING'
    else:
        query = query.replace('%s', '?')
        if 'ON CONFLICT DO NOTHING' in query and 'INSERT OR IGNORE' not in query:
            query = query.replace('INSERT INTO', 'INSERT OR IGNORE INTO').replace(' ON CONFLICT DO NOTHING', '')
            
    cursor.execute(query, params)


def init_db(schema_file='schema.sql'):
    """Applies schema.sql to the database."""
    schema_file = _resolve(schema_file)
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
