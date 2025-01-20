from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields
import pyodbc
import re
import yaml
from flask_cors import CORS
from loguru import logger
import os

# Initialize Flask app and extensions
app = Flask(__name__)
CORS(app)
app.json.sort_keys = False

# Set up Flask-RESTx API
api = Api(
    app,
    version="1.0",
    title="DB Scanner API",
    description="An API for scanning database metadata.",
    doc="/docs"  # Swagger UI available at root
)

# Namespace for metadata-related operations
ns_metadata = api.namespace("", description="Database Metadata Operations")

# Ensure the logs directory exists
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure the logger
logger.add(
    os.path.join(log_directory, "dbscanner.log"),
    rotation="1 week",
    level="INFO",
    compression="zip"
)
logger.info("Logger initialized successfully!")

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

# Construct the connection string
connection_string = (
    f"Driver={db_config.get('driver', '')};"
    f"Server={db_config.get('server', '')};"
    f"Database={db_config.get('database', '')};"
    f"UID={db_config.get('uid', '')};"
    f"PWD={db_config.get('pwd', '')};"
)

# Load SQL queries
def load_sql_query(filename):
    file_path = os.path.join("queries", filename)
    try:
        with open(file_path, "r") as file:
            return file.read()
    except Exception as e:
        logger.error(f"Error loading SQL query from {filename}: {str(e)}")
        raise ValueError(f"Failed to load query file: {filename}")

try:
    metadata_query = load_sql_query("get_table_metadata.sql")
    parent_foreign_key_query = load_sql_query("get_parent_foreign_keys.sql")
    child_foreign_key_query = load_sql_query("get_child_foreign_keys.sql")
    count_query = load_sql_query("get_record_count.sql")
except ValueError as ve:
    logger.error(f"Query file loading failed: {str(ve)}")
    raise

# Helper functions
def sanitize_table_name(table_name):
    """Basic sanitization to prevent SQL injection."""
    if not re.match("^[A-Za-z0-9_]+$", table_name):
        raise ValueError("Invalid table name. Only alphanumeric characters and underscores are allowed.")

def row_to_dict(row):
    """Converts a pyodbc.Row object to a dictionary."""
    return {column[0]: value for column, value in zip(row.cursor_description, row)}

def get_table_metadata(table_name):
    """Fetch metadata for a given table."""
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(metadata_query, (table_name, table_name, table_name))
    metadata = cursor.fetchall()
    formatted_metadata = [
        {
            "COLUMN_NAME": row.COLUMN_NAME,
            "DATA_TYPE": row.DATA_TYPE,
            "CHARACTER_MAXIMUM_LENGTH": row.CHARACTER_MAXIMUM_LENGTH,
            "PRIMARY_KEY": bool(row.PRIMARY_KEY),
            "NULLABLE": bool(row.NULLABLE),
            "IDENTITY": bool(row.IS_IDENTITY)
        }
        for row in metadata
    ]
    conn.close()
    return formatted_metadata

def get_foreign_keys(query, table_name):
    """Get foreign keys for a given table using the provided query."""
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(query, (table_name,))
    data = cursor.fetchall()
    conn.close()
    return [row_to_dict(row) for row in data]

def get_record_count(table_name):
    """Fetch the number of rows for a given table."""
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(count_query, (table_name,))
    count = cursor.fetchone()[0]
    conn.close()
    return count

# API Models
table_metadata_model = api.model("TableMetadata", {
    "table_name": fields.String(required=True, description="Name of the table"),
    "total_rows": fields.Integer(description="Total rows in the table"),
    "columns": fields.List(fields.Raw, description="Column metadata")
})

constraint_model = api.model("Constraint", {
    "ConstraintName": fields.String(description="Constraint name"),
    "ChildTable": fields.String(description="Child table name"),
    "ChildColumn": fields.String(description="Child column name"),
    "ReferencedTable": fields.String(description="Referenced table name"),
    "ReferencedColumn": fields.String(description="Referenced column name")
})

metadata_response_model = api.model("MetadataResponse", {
    "central_table_metadata": fields.List(fields.Nested(table_metadata_model), description="Metadata for the central table"),
    "parent_tables_metadata": fields.List(fields.Nested(table_metadata_model), description="Metadata for parent tables"),
    "child_tables_metadata": fields.List(fields.Nested(table_metadata_model), description="Metadata for child tables"),
    "constraint_details": fields.List(fields.Nested(constraint_model), description="Constraint details")
})

# API Endpoints
@ns_metadata.route("/metadata")
class Metadata(Resource):
    @api.response(200, "Success", metadata_response_model)
    @api.response(400, "Invalid Request")
    @api.response(500, "Internal Server Error")
    @api.doc(params={"table_name": "The name of the table to fetch metadata for"})
    def get(self):
        """Fetch metadata for a table, including parent and child tables and constraints."""
        table_name = request.args.get("table_name")
        if not table_name:
            return {"error": "Table name is required"}, 400
        try:
            sanitize_table_name(table_name)
        except ValueError as ve:
            return {"error": str(ve)}, 400

        try:
            central_metadata = get_table_metadata(table_name)
            total_rows = get_record_count(table_name)
            central_table = {
                "table_name": table_name,
                "total_rows": total_rows,
                "columns": central_metadata
            }

            parent_foreign_keys = get_foreign_keys(parent_foreign_key_query, table_name)
            parent_tables = [
                {
                    "table_name": key["ReferencedTable"],
                    "total_rows": get_record_count(key["ReferencedTable"]),
                    "columns": get_table_metadata(key["ReferencedTable"])
                }
                for key in parent_foreign_keys
            ]

            child_foreign_keys = get_foreign_keys(child_foreign_key_query, table_name)
            child_tables = [
                {
                    "table_name": key["ChildTable"],
                    "total_rows": get_record_count(key["ChildTable"]),
                    "columns": get_table_metadata(key["ChildTable"])
                }
                for key in child_foreign_keys
            ]

            constraints = parent_foreign_keys + child_foreign_keys

            return {
                "central_table_metadata": [central_table],
                "parent_tables_metadata": parent_tables,
                "child_tables_metadata": child_tables,
                "constraint_details": constraints
            }, 200
        except Exception as e:
            logger.error(f"Error fetching metadata: {str(e)}")
            return {"error": "Internal server error"}, 500

# Run the app
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
