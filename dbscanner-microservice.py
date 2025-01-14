from flask import Flask, request, jsonify
import pyodbc
import re
import yaml
from flask_cors import CORS
from loguru import logger
import os

app = Flask(__name__)
CORS(app)

# Ensure the logs directory exists
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure the logger to log to 'dbscanner.log'
logger.add(
    os.path.join(log_directory, "dbscanner.log"),  # Log file path
    rotation="1 week",  # Log rotation based on time
    level="INFO",  # Log level
    compression="zip"  # Optional: compress log files when rotated
)

# Example of logging a test message
logger.info("Logger initialized successfully!")

app.json.sort_keys = False

# Load config from YAML
def load_config():
    try:
        with open("appconfig.yml", "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading config file: {str(e)}")
        raise ValueError("Failed to load config file.")

config = load_config()
db_config = config.get("sql-server-database", {})

# Construct the connection string dynamically
connection_string = (
    f"Driver={db_config.get('driver', '')};"
    f"Server={db_config.get('server', '')};"
    f"Database={db_config.get('database', '')};"
    f"UID={db_config.get('uid', '')};"
    f"PWD={db_config.get('pwd', '')};"
)

# Function to load SQL queries from files
def load_sql_query(filename):
    file_path = os.path.join("queries", filename)
    try:
        with open(file_path, "r") as file:
            return file.read()
    except Exception as e:
        logger.error(f"Error loading SQL query from {filename}: {str(e)}")
        raise ValueError(f"Failed to load query file: {filename}")

# Queries loaded from SQL files
try:
    metadata_query = load_sql_query("get_table_metadata.sql")
    parent_foreign_key_query = load_sql_query("get_parent_foreign_keys.sql")
    child_foreign_key_query = load_sql_query("get_child_foreign_keys.sql")
    count_query = load_sql_query("get_record_count.sql")
except ValueError as ve:
    logger.error(f"Query file loading failed: {str(ve)}")
    raise

def sanitize_table_name(table_name):
    """Basic sanitization to prevent SQL injection"""
    logger.info(f"Sanitizing table name: {table_name}")  # Log the table name
    if not re.match("^[A-Za-z0-9_]+$", table_name):  # Basic alphanumeric validation
        logger.error(f"Invalid table name provided: {table_name}")
        raise ValueError("Invalid table name. Only alphanumeric characters and underscores are allowed.")

def row_to_dict(row):
    """Converts a pyodbc.Row object to a dictionary."""
    return {column[0]: value for column, value in zip(row.cursor_description, row)}

def get_table_metadata(table_name):
    """Fetch metadata for a given table."""
    logger.info(f"Fetching metadata for table: {table_name}")
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(metadata_query, (table_name, table_name, table_name))
        metadata = cursor.fetchall()

        formatted_metadata = []
        for row in metadata:
            formatted_metadata.append({
                "COLUMN_NAME": row.COLUMN_NAME,
                "DATA_TYPE": row.DATA_TYPE,
                "CHARACTER_MAXIMUM_LENGTH": row.CHARACTER_MAXIMUM_LENGTH,
                "PRIMARY_KEY": bool(row.PRIMARY_KEY),
                "NULLABLE": bool(row.NULLABLE),
                "IDENTITY": bool(row.IS_IDENTITY)
            })

        conn.close()
        logger.info(f"Fetched metadata for table: {table_name}")
        return formatted_metadata
    except pyodbc.Error as e:
        logger.error(f"Database error fetching table metadata for {table_name}: {str(e)}")
        return str(e)
    except Exception as e:
        logger.error(f"Unexpected error fetching table metadata for {table_name}: {str(e)}")
        return str(e)

def get_foreign_keys(query, table_name):
    """Get foreign keys for a given table using the provided query."""
    logger.info(f"Fetching foreign keys for table: {table_name}")
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(query, (table_name,))
        data = cursor.fetchall()
        data = [row_to_dict(row) for row in data]
        conn.close()
        logger.info(f"Fetched foreign keys for table: {table_name}")
        return data
    except pyodbc.Error as e:
        logger.error(f"Database error fetching foreign keys for {table_name}: {str(e)}")
        return str(e)
    except Exception as e:
        logger.error(f"Unexpected error fetching foreign keys for {table_name}: {str(e)}")
        return str(e)

def get_record_count(table_name):
    """Fetch the number of rows for a given table using the sys.dm_db_partition_stats view."""
    logger.info(f"Fetching row count for table: {table_name}")
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(count_query, (table_name,))
        count = cursor.fetchone()[0]
        conn.close()
        logger.info(f"Fetched row count for table: {table_name}: {count}")
        return count
    except pyodbc.Error as e:
        logger.error(f"Database error fetching row count for {table_name}: {str(e)}")
        return str(e)
    except Exception as e:
        logger.error(f"Unexpected error fetching row count for {table_name}: {str(e)}")
        return str(e)

@app.route('/get_metadata', methods=['GET'])
def get_metadata():
    """Endpoint to retrieve metadata for a table, its related parent and child tables, and constraints."""
    table_name = request.args.get('table_name')

    if not table_name:
        logger.warning("No table name provided in request.")
        return jsonify({"error": "Table name is required"}), 400

    try:
        sanitize_table_name(table_name)
    except ValueError as ve:
        logger.error(f"Invalid table name in request: {table_name}")
        return jsonify({"error": str(ve)}), 400

    # Get metadata for the central table
    central_metadata = get_table_metadata(table_name)
    if isinstance(central_metadata, str):  # Error occurred
        logger.error(f"Error fetching metadata for table: {table_name}")
        return jsonify({"error": central_metadata}), 500
    
    if not central_metadata:
        response_data = {
            "central_table_metadata": [],
            "parent_tables_metadata": [],
            "child_tables_metadata": [],
            "constraint_details": []
        }
        logger.info(f"No metadata found for table: {table_name}")
        return jsonify(response_data), 404

    # Get the row count for the central table
    total_rows = get_record_count(table_name)

    # Format central table metadata to include the table name and row count
    central_metadata_with_table_name = {
        "table_name": table_name,
        "total_rows": total_rows,
        "columns": central_metadata
    }

    # Get parent foreign keys and their metadata
    parent_foreign_keys = get_foreign_keys(parent_foreign_key_query, table_name)
    parent_metadata = []
    parent_constraints = []
    if parent_foreign_keys:
        logger.info(f"Found {len(parent_foreign_keys)} parent foreign keys for table: {table_name}")
        for parent in parent_foreign_keys:
            parent_table = parent['ReferencedTable']
            parent_table_metadata = get_table_metadata(parent_table)
            parent_table_rows = get_record_count(parent_table)  # Get row count for parent table
            if not isinstance(parent_table_metadata, str):
                parent_metadata.append({
                    "table_name": parent_table,
                    "total_rows": parent_table_rows,
                    "columns": parent_table_metadata
                })
            
            # Add constraint details in the preferred order
            parent_constraints.append({
                "ConstraintName": parent.get('ConstraintName'),
                "ChildTable": parent.get('ParentTable'),
                "ChildColumn": parent.get('ParentColumn'),
                "ReferencedTable": parent.get('ReferencedTable'),
                "ReferencedColumn": parent.get('ReferencedColumn')
            })

    # Get child foreign keys and their metadata
    child_foreign_keys = get_foreign_keys(child_foreign_key_query, table_name)
    child_metadata = []
    child_constraints = []
    if child_foreign_keys:
        logger.info(f"Found {len(child_foreign_keys)} child foreign keys for table: {table_name}")
        for child in child_foreign_keys:
            child_table = child['ChildTable']
            child_table_metadata = get_table_metadata(child_table)
            child_table_rows = get_record_count(child_table)  # Get row count for child table
            if not isinstance(child_table_metadata, str):
                child_metadata.append({
                    "table_name": child_table,
                    "total_rows": child_table_rows,
                    "columns": child_table_metadata
                })
            
            # Add constraint details in the preferred order
            child_constraints.append({
                "ConstraintName": child.get('ConstraintName'),
                "ChildTable": child.get('ChildTable'),
                "ChildColumn": child.get('ChildColumn'),
                "ReferencedTable": child.get('ReferencedTable'),
                "ReferencedColumn": child.get('ReferencedColumn')
            })

    # Combine all metadata
    response_data = {
        "central_table_metadata": [central_metadata_with_table_name],
        "parent_tables_metadata": parent_metadata,
        "child_tables_metadata": child_metadata,
        "constraint_details": parent_constraints + child_constraints
    }

    logger.info(f"Returning metadata for table: {table_name}")
    return jsonify(response_data), 200

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
