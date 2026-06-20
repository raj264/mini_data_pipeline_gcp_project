import ftplib
from unittest.mock import MagicMock, mock_open, patch

import pytest

from ftp_ingest import download_from_ftp, upload_to_gcs


def test_download_from_ftp_connects_and_retrieves():
    with patch("ftplib.FTP") as mock_ftp_class, patch("builtins.open", mock_open()):
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp

        download_from_ftp("ftp.example.com", 21, "user", "pass", "/remote/file.csv", "/tmp/file.csv")

        mock_ftp.connect.assert_called_once_with("ftp.example.com", 21, timeout=30)
        mock_ftp.login.assert_called_once_with("user", "pass")
        mock_ftp.retrbinary.assert_called_once()
        mock_ftp.quit.assert_called_once()


def test_download_from_ftp_retries_then_raises():
    with patch("ftplib.FTP") as mock_ftp_class, patch("time.sleep", return_value=None):
        mock_ftp = MagicMock()
        mock_ftp.connect.side_effect = ftplib.error_temp("421 timeout")
        mock_ftp_class.return_value = mock_ftp

        with pytest.raises(ftplib.error_temp):
            download_from_ftp("ftp.example.com", 21, "user", "pass", "/remote/file.csv", "/tmp/file.csv")

        assert mock_ftp.connect.call_count == 3


def test_upload_to_gcs_uploads_file():
    with patch("ftp_ingest.storage.Client") as mock_client_class:
        mock_blob = MagicMock()
        mock_client_class.return_value.bucket.return_value.blob.return_value = mock_blob

        upload_to_gcs("/tmp/file.csv", "my-bucket", "raw/file.csv")

        mock_blob.upload_from_filename.assert_called_once_with("/tmp/file.csv")
