import unittest
from unittest.mock import patch, MagicMock
from de_lib.ingestion_manager import KaggleIngestionManager, URIIngestionManager, IngestionManager

class TestKaggleIngestionManager(unittest.TestCase):
    @patch('de_lib.ingestion_manager.KaggleApi')
    def test_download_dataset(self, MockKaggleApi):
        mock_api = MockKaggleApi.return_value
        manager = KaggleIngestionManager()
        manager.download_dataset('dataset', 'download_path')
        mock_api.dataset_download_files.assert_called_once_with('dataset', path='download_path', unzip=True)

class TestURIIngestionManager(unittest.TestCase):
    @patch('de_lib.ingestion_manager.requests.get')
    def test_download_dataset(self, mock_get):
        mock_response = MagicMock()
        mock_response.content = b'data'
        mock_get.return_value = mock_response
        manager = URIIngestionManager()
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            manager.download_dataset('http://example.com/dataset', 'download_path')
            mock_get.assert_called_once_with('http://example.com/dataset')
            mock_file.assert_called_once_with('download_path/dataset', 'wb')
            mock_file().write.assert_called_once_with(b'data')

class TestIngestionManager(unittest.TestCase):
    def test_get_kaggle_ingestion_manager(self):
        manager = IngestionManager().get_ingestion_manager('kaggle')
        self.assertIsInstance(manager, KaggleIngestionManager)

    def test_get_uri_ingestion_manager(self):
        manager = IngestionManager().get_ingestion_manager('uri')
        self.assertIsInstance(manager, URIIngestionManager)

    def test_invalid_ingestion_type(self):
        with self.assertRaises(ValueError):
            IngestionManager().get_ingestion_manager('invalid')

if __name__ == '__main__':
    unittest.main()
