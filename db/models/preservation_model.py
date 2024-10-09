from db.models import get_db_connection

# Function to initialize the database schema
def init_db():
    with get_db_connection() as conn:
        # Create the preservation_configs table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS preservation_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                process_type TEXT CHECK(process_type IN ('standard', 'eark')) DEFAULT 'standard' NOT NULL,
                compress_aip INTEGER DEFAULT 0,
                gen_transfer_struct_report INTEGER DEFAULT 0,
                document_empty_directories INTEGER DEFAULT 0,
                extract_packages INTEGER DEFAULT 0,
                delete_packages_after_extraction INTEGER DEFAULT 0,
                normalize INTEGER DEFAULT 0,
                compression_level INTEGER CHECK(compression_level BETWEEN 1 AND 9) DEFAULT 1,
                compression_algorithm TEXT CHECK(compression_algorithm IN ('tar', 'tar_bzip2', 'tar_gzip', 's7_copy', 's7_bzip2', 's7_lzma')) DEFAULT 's7_bzip2',
                image_normalization_tiff INTEGER DEFAULT 0,
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                user TEXT NOT NULL,
                dip_enabled INTEGER DEFAULT 0
            );
        """)

        if not conn.execute("SELECT 1 FROM preservation_configs LIMIT 1;").fetchone():
            conn.execute("""
                INSERT INTO preservation_configs (
                    name, process_type, compress_aip, gen_transfer_struct_report,
                    document_empty_directories, extract_packages, delete_packages_after_extraction,
                    normalize, compression_level, compression_algorithm, image_normalization_tiff,
                    description, user, dip_enabled
                ) VALUES (
                    'Default', 'standard', 0, 0, 0, 0, 0, 1, 1, 's7_bzip2', 0, "Default Config (Can't be deleted)", 'System', 0
                );
            """)
        
        # Check if the trigger already exists
        trigger_exists = conn.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='trigger' AND name='update_modified_timestamp';
        """).fetchone() is not None
        
        # Configure the trigger to update the modified timestamp
        if not trigger_exists:
            conn.execute("""
                CREATE TRIGGER update_modified_timestamp
                AFTER UPDATE ON preservation_configs
                FOR EACH ROW
                BEGIN
                    UPDATE preservation_configs
                    SET modified = CURRENT_TIMESTAMP
                    WHERE id = OLD.id;
                END;
            """)
        
        conn.commit()

class PreservationConfigModel:
    def __init__(self, id: int, name: str, process_type: str, compress_aip: int, gen_transfer_struct_report: int, document_empty_directories: int, extract_packages: int, delete_packages_after_extraction: int, normalize: int, compression_level: int, compression_algorithm: str, image_normalization_tiff: int, description: str, user: str, dip_enabled: int):
        self.id = id
        self.name = name
        self.process_type = process_type
        self.compress_aip = compress_aip
        self.gen_transfer_struct_report = gen_transfer_struct_report
        self.document_empty_directories = document_empty_directories
        self.extract_packages = extract_packages
        self.delete_packages_after_extraction = delete_packages_after_extraction
        self.normalize = normalize
        self.compression_level = compression_level
        self.compression_algorithm = compression_algorithm
        self.image_normalization_tiff = image_normalization_tiff
        self.description = description
        self.user = user
        self.dip_enabled = dip_enabled
        
    def add_new_config_to_db(data: dict):
        with get_db_connection() as conn:
            conn.execute('''
                INSERT INTO preservation_configs (name, process_type, compress_aip, gen_transfer_struct_report,
                    document_empty_directories, extract_packages, delete_packages_after_extraction,
                    normalize, compression_level, compression_algorithm, image_normalization_tiff,
                    description, user, dip_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (data['name'], data['process_type'], data.get('compress_aip', 0),
                data.get('gen_transfer_struct_report', 0), data.get('document_empty_directories', 0),
                data.get('extract_packages', 0), data.get('delete_packages_after_extraction', 0),
                data.get('normalize', 0), data.get('compression_level', 1),
                data.get('compression_algorithm', 's7_bzip2'), data.get('image_normalization_tiff', 0),
                data.get('description', None), data['user'], data.get('dip_enabled', 0)))
            conn.commit()

    def update_config_in_db(data: dict, id: int):
        with get_db_connection() as conn:
            conn.execute('''
                UPDATE preservation_configs
                SET name=?, process_type=?, compress_aip=?, gen_transfer_struct_report=?, 
                    document_empty_directories=?, extract_packages=?, delete_packages_after_extraction=?,
                    normalize=?, compression_level=?, compression_algorithm=?, image_normalization_tiff=?,
                    modified=CURRENT_TIMESTAMP, description=?, user=?, dip_enabled=?
                WHERE id=?
            ''', (data['name'], data['process_type'], data.get('compress_aip', 0),
                data.get('gen_transfer_struct_report', 0), data.get('document_empty_directories', 0),
                data.get('extract_packages', 0), data.get('delete_packages_after_extraction', 0),
                data.get('normalize', 0), data.get('compression_level', 1),
                data.get('compression_algorithm', 's7_bzip2'), data.get('image_normalization_tiff', 0),
                data.get('description', None), data['user'], data.get('dip_enabled', 0), id))
            conn.commit()

    def get_config_from_db(id: int) -> dict:
        with get_db_connection() as conn:
            cursor = conn.execute('SELECT * FROM preservation_configs WHERE id = ? LIMIT 1', (id,))
            config = cursor.fetchone()
            return config if config else None
    
    def get_all_configs_from_db() -> dict:
        with get_db_connection() as conn:
            cursor = conn.execute('SELECT * FROM preservation_configs')
            configs = [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
            return configs
    
    def delete_config_from_db(id: int):
        if id == 1:
            return
        with get_db_connection() as conn:
            conn.execute('DELETE FROM preservation_configs WHERE id = ?', (id,))
            conn.commit()
