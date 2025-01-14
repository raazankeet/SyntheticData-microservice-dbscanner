import unittest
from dbscanner_microservice import app  # Import the Flask app from your app file
import pyodbc  # Import pyodbc to patch it in the tests
from unittest.mock import patch

class TestApp(unittest.TestCase):

    def setUp(self):
        # Set up the test client
        self.client = app.test_client()

    def test_missing_table_name(self):
        """Test if the API returns an error when no table name is provided."""
        response = self.client.get('/get_metadata')
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json)
        self.assertEqual(response.json['error'], 'Table name is required')

    def test_invalid_table_name(self):
        """Test if the API returns an error for an invalid table name."""
        with patch('dbscanner_microservice.sanitize_table_name', side_effect=ValueError('Invalid table name')):
            response = self.client.get('/get_metadata?table_name=invalid_table!')
            self.assertEqual(response.status_code, 400)
            self.assertIn('error', response.json)
            self.assertEqual(response.json['error'], 'Invalid table name')  # Updated expected error message


    def test_table_not_found(self):
        """Test if the API returns an error when the table doesn't exist in the catalog."""
        with patch('dbscanner_microservice.get_table_metadata', return_value=[]):
            response = self.client.get('/get_metadata?table_name=nonexistent_table')
            self.assertEqual(response.status_code, 400)
            self.assertIn('error', response.json)
            self.assertEqual(response.json['error'], 'Table nonexistent_table doesn\'t exist in catalog!')

    def test_valid_table_with_metadata(self):
        """Test if the API returns metadata for a valid table."""
        mock_metadata = [{
            "COLUMN_NAME": "id",
            "DATA_TYPE": "int",
            "CHARACTER_MAXIMUM_LENGTH": None,
            "PRIMARY_KEY": True,
            "NULLABLE": False,
            "IDENTITY": True
        }]
        with patch('dbscanner_microservice.get_table_metadata', return_value=mock_metadata):
            with patch('dbscanner_microservice.get_record_count', return_value=100):
                response = self.client.get('/get_metadata?table_name=valid_table')
                self.assertEqual(response.status_code, 200)
                self.assertIn('central_table_metadata', response.json)
                self.assertEqual(response.json['central_table_metadata'][0]['table_name'], 'valid_table')
                self.assertEqual(response.json['central_table_metadata'][0]['total_rows'], 100)
                self.assertEqual(len(response.json['parent_tables_metadata']), 0)
                self.assertEqual(len(response.json['child_tables_metadata']), 0)

def test_database_error(self):
    """Test if the API handles database connection errors gracefully."""
    with patch('pyodbc.connect', side_effect=pyodbc.Error('Database connection error')):
        response = self.client.get('/get_metadata?table_name=valid_table')
        self.assertEqual(response.status_code, 500)
        self.assertIn('error', response.json)
        self.assertIn('Database error', response.json['error'])  # Partial match

    def test_valid_table_with_foreign_keys(self):
        """Test if the API returns metadata for a table with foreign keys."""
        mock_metadata = [{
            "COLUMN_NAME": "id",
            "DATA_TYPE": "int",
            "CHARACTER_MAXIMUM_LENGTH": None,
            "PRIMARY_KEY": True,
            "NULLABLE": False,
            "IDENTITY": True
        }]
        mock_parent_foreign_keys = [{
            'ConstraintName': 'FK_Parent_Child',
            'ParentTable': 'parent_table',
            'ParentColumn': 'parent_id',
            'ReferencedTable': 'referenced_table',
            'ReferencedColumn': 'referenced_id'
        }]
        with patch('dbscanner_microservice.get_table_metadata', return_value=mock_metadata):
            with patch('dbscanner_microservice.get_foreign_keys', side_effect=lambda query, table_name: mock_parent_foreign_keys if 'parent' in table_name else []):
                with patch('dbscanner_microservice.get_record_count', return_value=100):
                    response = self.client.get('/get_metadata?table_name=child_table')
                    self.assertEqual(response.status_code, 200)
                    self.assertIn('parent_tables_metadata', response.json)
                    self.assertEqual(len(response.json['parent_tables_metadata']), 1)
                    self.assertEqual(response.json['parent_tables_metadata'][0]['table_name'], 'parent_table')
                    self.assertEqual(len(response.json['child_tables_metadata']), 0)

if __name__ == '__main__':
    unittest.main()
