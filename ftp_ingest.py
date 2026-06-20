"""Module: ftp_ingest.py
Handles batch ingestion from FTP/SFTP into Cloud Storage."""
import ftplib
from google.cloud import storage

def download_from_ftp(host: str, port: int, username: str, password: str,
                      remote_path: str, local_path: str) -> None:
    """Download a file from FTP/SFTP to local path."""
    ftp = ftplib.FTP()
    ftp.connect(host, port)
    ftp.login(username, password)
    with open(local_path, 'wb') as f:
        ftp.retrbinary(f'RETR {remote_path}', f.write)
    ftp.quit()

def upload_to_gcs(local_path: str, bucket_name: str, destination_blob_name: str) -> None:
    """Upload a local file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_path)
