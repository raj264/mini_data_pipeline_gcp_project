"""Module: ftp_ingest.py
Handles batch ingestion from FTP/SFTP into Cloud Storage."""
import ftplib
import logging
import time

from google.cloud import storage

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
BACKOFF_SECONDS = 2


def download_from_ftp(host: str, port: int, username: str, password: str,
                      remote_path: str, local_path: str) -> None:
    """Download a file from FTP/SFTP to local path, retrying on transient failures."""
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        ftp = ftplib.FTP()
        try:
            ftp.connect(host, port, timeout=30)
            ftp.login(username, password)
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {remote_path}', f.write)
            return
        except ftplib.all_errors as e:
            last_error = e
            logger.error("FTP download failed (attempt %d/%d): %s", attempt, MAX_ATTEMPTS, e)
            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF_SECONDS * attempt)
        finally:
            try:
                ftp.quit()
            except Exception:
                pass

    raise last_error


def upload_to_gcs(local_path: str, bucket_name: str, destination_blob_name: str) -> None:
    """Upload a local file to Google Cloud Storage, retrying on transient failures."""
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_path)
            return
        except Exception as e:
            last_error = e
            logger.error("GCS upload failed (attempt %d/%d): %s", attempt, MAX_ATTEMPTS, e)
            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF_SECONDS * attempt)

    raise last_error
