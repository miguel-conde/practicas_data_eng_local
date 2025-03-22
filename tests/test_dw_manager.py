import unittest
from unittest.mock import patch, MagicMock
from de_lib.dw_manager import PGManager, SQLiteManager, DWManager

class TestPGManager(unittest.TestCase):
    @patch('de_lib.dw_manager.psycopg2.connect')
    def test_connect(self, mock_connect):
        manager = PGManager()
        manager.connect()
        mock_connect.assert_called_once()

    @patch('de_lib.dw_manager.psycopg2.connect')
    def test_execute_query(self, mock_connect):
        manager = PGManager()
        manager.connect()
        manager.cursor = MagicMock()
        manager.execute_query('SELECT 1')
        manager.cursor.execute.assert_called_once_with('SELECT 1', None)

    @patch('de_lib.dw_manager.psycopg2.connect')
    def test_fetch_data(self, mock_connect):
        manager = PGManager()
        manager.connect()
        manager.cursor = MagicMock()
        manager.cursor.fetchall.return_value = [('data',)]
        result = manager.fetch_data('SELECT 1')
        manager.cursor.execute.assert_called_once_with('SELECT 1', None)
        self.assertEqual(result, [('data',)])

class TestSQLiteManager(unittest.TestCase):
    @patch('de_lib.dw_manager.sqlite3.connect')
    def test_connect(self, mock_connect):
        manager = SQLiteManager('db_path')
        manager.connect()
        mock_connect.assert_called_once_with('db_path')

    @patch('de_lib.dw_manager.sqlite3.connect')
    def test_execute_query(self, mock_connect):
        manager = SQLiteManager('db_path')
        manager.connect()
        manager.cursor = MagicMock()
        manager.execute_query('SELECT 1')
        manager.cursor.execute.assert_called_once_with('SELECT 1', None)

    @patch('de_lib.dw_manager.sqlite3.connect')
    def test_fetch_data(self, mock_connect):
        manager = SQLiteManager('db_path')
        manager.connect()
        manager.cursor = MagicMock()
        manager.cursor.fetchall.return_value = [('data',)]
        result = manager.fetch_data('SELECT 1')
        manager.cursor.execute.assert_called_once_with('SELECT 1', None)
        self.assertEqual(result, [('data',)])

class TestDWManager(unittest.TestCase):
    # @patch('de_lib.dw_manager.PGManager')
    def test_create_pg_manager(self):
        manager = DWManager.create_dw_manager('postgres')
        self.assertIsInstance(manager, PGManager)

    # @patch('de_lib.dw_manager.SQLiteManager')
    def test_create_sqlite_manager(self):
        manager = DWManager.create_dw_manager('sqlite')
        self.assertIsInstance(manager, SQLiteManager)

    def test_invalid_dw_type(self):
        with self.assertRaises(ValueError):
            DWManager.create_dw_manager('invalid')

if __name__ == '__main__':
    unittest.main()
