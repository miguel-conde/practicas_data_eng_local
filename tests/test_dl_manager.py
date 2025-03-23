import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from de_lib.dl_manager import MinioDLManager, LocalFolderDLManager, DLManager

class TestMinioDLManager(unittest.TestCase):
    @patch('de_lib.dl_manager.Minio')
    def test_get_csv(self, MockMinio):
        mock_client = MockMinio.return_value
        mock_client.get_object.return_value = MagicMock()
        with patch('os.path.exists', return_value=True):
            manager = MinioDLManager()
            with patch('pandas.read_csv') as mock_read_csv:
                manager.get_csv('object_name')
                mock_client.get_object.assert_called_once_with(manager._minio_bucket, 'object_name')
                mock_read_csv.assert_called_once()

    @patch('de_lib.dl_manager.Minio')
    def test_put_csv(self, MockMinio):
        mock_client = MockMinio.return_value
        manager = MinioDLManager()
        df = pd.DataFrame({'col': [1, 2, 3]})
        with patch('io.BytesIO') as mock_bytes_io:
            manager.put_csv(df, 'object_name')
            mock_client.put_object.assert_called_once()

    
    @patch('os.stat')
    @patch('os.path.exists', return_value=True)
    @patch('de_lib.dl_manager.Minio')
    def test_put_file(self, MockMinio, mock_exists, mock_stat):
        mock_client = MockMinio.return_value
        mock_stat.return_value.st_size = 123  # Mock the file size
        manager = MinioDLManager()
        with patch('builtins.open', unittest.mock.mock_open(read_data='data')) as mock_file:
            manager.put_file('file_path', 'object_name')
            mock_client.put_object.assert_called_once_with(
                bucket_name=manager._minio_bucket,
                object_name='object_name',
                data=mock_file(),
                length=123,
                content_type='application/csv'
            )


    @patch('de_lib.dl_manager.Minio')
    def test_file_exists(self, MockMinio):
        mock_client = MockMinio.return_value
        manager = MinioDLManager()
        mock_client.stat_object.return_value = True
        self.assertTrue(manager.file_exists('object_name'))

class TestLocalFolderDLManager(unittest.TestCase):
    @patch('de_lib.dl_manager.Path.mkdir')
    @patch('os.path.exists', return_value=True)
    def test_get_csv(self, mock_exists, mock_mkdir):
        manager = LocalFolderDLManager('folder_path')
        with patch('pandas.read_csv') as mock_read_csv:
            with patch('builtins.open', unittest.mock.mock_open(read_data='data')):
                manager.get_csv('object_name')
                mock_read_csv.assert_called_once()

    @patch('de_lib.dl_manager.Path.mkdir')
    def test_put_csv(self, mock_mkdir):
        manager = LocalFolderDLManager('folder_path')
        df = pd.DataFrame({'col': [1, 2, 3]})
        with patch('pandas.DataFrame.to_csv') as mock_to_csv:
            manager.put_csv(df, 'object_name')
            mock_to_csv.assert_called_once()

    @patch('de_lib.dl_manager.Path.mkdir')
    def test_put_file(self, mock_mkdir):
        manager = LocalFolderDLManager('folder_path')
        with patch('shutil.copyfile') as mock_copyfile:
            manager.put_file('file_path', 'object_name')
            mock_copyfile.assert_called_once()

    @patch('de_lib.dl_manager.Path.mkdir')
    def test_file_exists(self, mock_mkdir):
        manager = LocalFolderDLManager('folder_path')
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            self.assertTrue(manager.file_exists('object_name'))

class TestDLManager(unittest.TestCase):
    # @patch('de_lib.dl_manager.MinioDLManager')
    def test_create_minio_manager(self):
        manager = DLManager.create_dl_manager('minio')
        self.assertIsInstance(manager, MinioDLManager)

    # @patch('de_lib.dl_manager.LocalFolderDLManager')
    def test_create_local_folder_manager(self):
        manager = DLManager.create_dl_manager('local_folder', folder_path="tmp_dir")
        self.assertIsInstance(manager, LocalFolderDLManager)

    def test_invalid_manager_type(self):
        with self.assertRaises(ValueError):
            DLManager.create_dl_manager('invalid')

if __name__ == '__main__':
    unittest.main()
