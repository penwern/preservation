from db.models import get_db_connection

# Function to initialize the database schema
def init_db():
    with get_db_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS atom_config (
                id INTEGER PRIMARY KEY,
                atom_url TEXT NOT NULL,
                atom_api_key TEXT NOT NULL,
                atom_username TEXT NOT NULL,
                atom_password TEXT NOT NULL
            );
        """)
        conn.commit()

class AtomConfigModel:
    def __init__(self, id: int, atom_url: str, atom_api_key: str):
        self.id = id
        self.atom_url = atom_url
        self.atom_api_key = atom_api_key
        
    def get_config_from_db() -> dict:
        with get_db_connection() as conn:
            cursor = conn.execute('SELECT * FROM atom_config LIMIT 1')
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

    def add_new_config_to_db(data: dict):
        with get_db_connection() as conn:
            conn.execute('''
                INSERT INTO atom_config (atom_url, atom_api_key, atom_username, atom_password)
                VALUES (?, ?, ?, ?)
            ''', (data['atom_url'], data['atom_api_key'], data['atom_username'], data['atom_password']))
            conn.commit()

    def update_config_in_db(data: dict):
        with get_db_connection() as conn:
            conn.execute('''
                UPDATE atom_config
                SET atom_url=?, atom_api_key=?, atom_username=?, atom_password=?
                WHERE id=?
            ''', (data['atom_url'], data['atom_api_key'], data['atom_username'], data['atom_password'], 1))
            conn.commit()
