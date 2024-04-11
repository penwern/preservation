import os
import sqlite3
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
# from wtforms.validators import Range, AnyOf, Length
from marshmallow import Schema, fields, ValidationError
from marshmallow.validate import Length, OneOf, Range


logger = logging.getLogger("preservation_api")
logger.setLevel(logging.INFO)
# Remove existing handlers to prevent duplicate logging
logger.handlers.clear()
file_handler = logging.FileHandler(f'/var/cells/penwern/logs/preservation_api_{str(datetime.now().date())}.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
# Don't maintain logger
logger.propagate = False

app = Flask(__name__)
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["120 per hour"],
    storage_uri="memory://",
)

# Expected location of the sqlite3 database.
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/preservation.db'))

# Requests should come directly from the specified domain.
# TODO Change domain origin.
ALLOWED_ORIGIN = 'https://www.example.co.uk'

# BAD EXAMPLES. DON'T DO THIS! PRONE TO SQL INJECTION.
# cursor.execute("SELECT admin FROM users WHERE username = '" + username + '");
# cursor.execute("SELECT admin FROM users WHERE username = '%s' % username);
# cursor.execute("SELECT admin FROM users WHERE username = '{}'".format(username));
# cursor.execute(f"SELECT admin FROM users WHERE username = '{username}'");

# Marshmallow Schema for JSON payload validation
class ConfigSchema(Schema):
    name = fields.String(required=True, validate=Length(max=255))
    process_type = fields.String(required=True, validate=OneOf(['standard', 'eark']))
    compress_aip = fields.Integer(validate=Range(min=0, max=1))
    gen_transfer_struct_report = fields.Integer(validate=Range(min=0, max=1))
    document_empty_directories = fields.Integer(validate=Range(min=0, max=1))
    extract_packages = fields.Integer(validate=Range(min=0, max=1))
    delete_packages_after_extraction = fields.Integer(validate=Range(min=0, max=1))
    normalize = fields.Integer(validate=Range(min=0, max=1))
    compression_level = fields.Integer(validate=Range(min=0))
    compression_algorithm = fields.String(validate=OneOf(['tar', 'tar_bzip2', 'tar_gzip', 's7_copy', 's7_bzip2', 's7_lzma']))
    image_normalization_tiff = fields.Integer(validate=Range(min=0, max=1))
    description = fields.String(validate=Length(max=255))
    user = fields.String(required=True, validate=Length(max=255))

# @app.before_request
# def check_referer():
#     # Referer header can be easily spoofed. Data is not sensitive.
#     referer = request.headers.get("Referer")
#     if referer is None or not referer.startswith(f"{ALLOWED_ORIGIN}/"):
#         return jsonify({"error": "Unauthorized"}), 401
    
@app.route("/")
@limiter.limit("10 per minute") 
def index():
    logger.info("Hello!")
    return "<h1>Hello!</h1>"

# Get all configs
@app.route('/configs', methods=['GET'])
@limiter.limit("10 per minute") 
def get_all_configs():
    logger.info("Getting all configs")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM preservation_configs')
    configs = cursor.fetchall()
    conn.close()
    logger.info(configs)
    return jsonify(configs)

# Get one config by ID
@app.route('/configs/<int:id>', methods=['GET'])
@limiter.limit("10 per minute") 
def get_config(id):
    logger.info(f"Getting config: {id}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM preservation_configs WHERE id = ?', (id,))
    config = cursor.fetchone()
    conn.close()
    if config:
        logger.info(config)
        return jsonify(config)
    else:
        return jsonify({'error': 'Config not found'}), 404

# Update existing config
@app.route('/configs/<int:id>', methods=['PUT'])
@limiter.limit("10 per minute") 
def update_config(id):
    logger.info(f"Updating config: {id}")
    
    data = request.json
    logger.info(data)
    
    # Validate JSON payload using Marshmallow schema
    try:
        v_data = ConfigSchema().load(data)
    except ValidationError as e:
        logger.info(f"Error: {e.messages}")
        return jsonify({'error': e.messages}), 400
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE preservation_configs
        SET name=?, process_type=?, compress_aip=?, gen_transfer_struct_report=?,
            document_empty_directories=?, extract_packages=?, delete_packages_after_extraction=?,
            normalize=?, compression_level=?, compression_algorithm=?, image_normalization_tiff=?,
            modified=CURRENT_TIMESTAMP, description=?, user=?
        WHERE id=?
    ''', (v_data['name'], v_data['process_type'], v_data.get('compress_aip', None),
          v_data.get('gen_transfer_struct_report', None), v_data.get('document_empty_directories', None),
          v_data.get('extract_packages', None), v_data.get('delete_packages_after_extraction', None),
          v_data.get('normalize', None), v_data.get('compression_level', None),
          v_data.get('compression_algorithm', None), v_data.get('image_normalization_tiff', None),
          v_data.get('description', None), v_data.get('user', None), id))
    conn.commit()
    conn.close()
    logger.info("Updated")
    return jsonify({'success': True})

# Add new config
@app.route('/configs', methods=['POST'])
@limiter.limit("10 per minute") 
def add_config():
    logger.info("Adding new config")
    data = request.json
    logger.info(data)
    
    # Validate JSON payload using Marshmallow schema
    try:
        v_data = ConfigSchema().load(data)
    except ValidationError as e:
        logger.info(f"Error: {e.messages}")
        return jsonify({'error': e.messages}), 400
    
    logger.info(v_data)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO preservation_configs (name, process_type, compress_aip, gen_transfer_struct_report,
            document_empty_directories, extract_packages, delete_packages_after_extraction,
            normalize, compression_level, compression_algorithm, image_normalization_tiff,
            description, user)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (v_data['name'], v_data['process_type'], v_data.get('compress_aip', None),
          v_data.get('gen_transfer_struct_report', None), v_data.get('document_empty_directories', None),
          v_data.get('extract_packages', None), v_data.get('delete_packages_after_extraction', None),
          v_data.get('normalize', None), v_data.get('compression_level', None),
          v_data.get('compression_algorithm', None), v_data.get('image_normalization_tiff', None),
          v_data.get('description', None), v_data.get('user', None)))
    conn.commit()
    conn.close()
    logger.info("Added")
    
    return jsonify({'success': True}), 201

# Delete config by ID
@app.route('/configs/<int:id>', methods=['DELETE'])
@limiter.limit("10 per minute")
def delete_config(id):
    logger.info(f"Deleting config: {id}")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if config with given ID exists
    cursor.execute('SELECT * FROM preservation_configs WHERE id = ?', (id,))
    config = cursor.fetchone()
    
    if config:
        # If config exists, delete it
        cursor.execute('DELETE FROM preservation_configs WHERE id = ?', (id,))
        conn.commit()
        conn.close()
        logger.info("Config deleted")
        return jsonify({'success': True})
    else:
        # If config does not exist, return error
        conn.close()
        logger.error(f"Config with ID {id} not found")
        return jsonify({'error': 'Config not found'}), 404
    
if __name__ == '__main__':
    app.run(debug=True)
