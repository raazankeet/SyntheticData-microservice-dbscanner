
# Synthetic Data Microservice - DB Scanner

This project is a microservice built with Flask, which connects to a SQL Server database, retrieves metadata for tables, and provides details about foreign key relationships, table structure, and row counts.

## Features

- **Fetch Table Metadata**: Get metadata information such as column names, data types, primary key status, nullability, and identity columns.
- **Foreign Key Relationships**: Retrieve parent and child foreign key relationships for a given table.
- **Row Count**: Fetch the number of rows in a table.
- **Configurable**: Database connection settings are stored in an external `appconfig.yml` file for easy management.

## Requirements

- Python 3.x
- Flask
- PyODBC
- Loguru
- Flask-CORS
- YAML

You can install the required dependencies using `pip`:

```bash
pip install -r requirements.txt
```

## Project Structure

```plaintext
/
├── app.py                   # Main Flask application
├── appconfig.yml            # Database configuration (should be kept private)
├── queries/                 # SQL queries folder
│   ├── get_table_metadata.sql
│   ├── get_parent_foreign_keys.sql
│   ├── get_child_foreign_keys.sql
│   └── get_record_count.sql
├── logs/                    # Logs directory
│   └── app.log              # Log file
└── requirements.txt         # Python dependencies
```

## Setup

### 1. Configure Database

Before running the application, ensure that you have configured the `appconfig.yml` file with the appropriate database connection settings:

```yaml
sql-server-database:
  driver: "ODBC Driver 17 for SQL Server"
  server: "your-server-name"
  database: "your-database-name"
  uid: "your-username"
  pwd: "your-password"
```

### 2. Start the Application

To start the Flask application, run the following command:

```bash
python app.py
```

The server will start running on `http://0.0.0.0:5000` by default.

### 3. Accessing the API

You can access the `/get_metadata` endpoint to fetch metadata for a table by providing the `table_name` as a query parameter:

```
GET /get_metadata?table_name=<your_table_name>
```

### Example Response

```json
{
  "central_table_metadata": [
    {
      "table_name": "users",
      "total_rows": 1000,
      "columns": [
        {
          "COLUMN_NAME": "id",
          "DATA_TYPE": "int",
          "CHARACTER_MAXIMUM_LENGTH": null,
          "PRIMARY_KEY": true,
          "NULLABLE": false,
          "IDENTITY": true
        },
        {
          "COLUMN_NAME": "name",
          "DATA_TYPE": "varchar",
          "CHARACTER_MAXIMUM_LENGTH": 255,
          "PRIMARY_KEY": false,
          "NULLABLE": false,
          "IDENTITY": false
        }
      ]
    }
  ],
  "parent_tables_metadata": [],
  "child_tables_metadata": [],
  "constraint_details": []
}
```

## Logging

Logs are stored in the `logs/` directory, and the application uses the `Loguru` library to log messages with different levels (e.g., `INFO`, `ERROR`).

Log files are rotated weekly, and older logs are compressed into `.zip` files.

## Error Handling

- If a table name is invalid (contains characters other than alphanumeric or underscores), the API will return a `400` status code with an error message.
- If any database query fails, the error message will be logged, and a `500` status code will be returned.
- If no metadata is found for a table, a `404` status code will be returned with an empty response.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
