import logging
import sqlite3 as sqlite


logger = logging.getLogger("preservation")

class DatabaseManager:
    def __init__(self, db_file):
        self.db_file = db_file

    def get_preservation_processing_configs(self, config_id):
        """
        Get processing config from database by ID.
        
        Returns processing config and a3m config.
        """
        with sqlite.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM preservation_configs WHERE id = ?", (config_id,))
            matching_row = cursor.fetchone()
            if not matching_row:
                raise ValueError(f"No matching row found for config id: {config_id}")
                        
        processing_config = {
            'id': matching_row[0],
            'name': matching_row[1],
            'description': matching_row[14],
            'process_type': matching_row[2],
            'compress_aip': bool(matching_row[3]),
            'image_normalization_tiff': bool(matching_row[11])
        }
        a3m_config = {
            "generate_transfer_structure_report": bool(matching_row[4]),
            "document_empty_directories": bool(matching_row[5]),
            "extract_packages": bool(matching_row[6]),
            "delete_packages_after_extraction": bool(matching_row[7]),
            "normalize": bool(matching_row[8]),
            "aip_compression_level": matching_row[9],
            "aip_compression_algorithm": matching_row[10],
        }
        logger.info(f"Loaded configs from database.")
        logger.debug(f"Processing config: {processing_config}.")
        logger.debug(f"A3M config: {a3m_config}.")
        return processing_config, a3m_config
            