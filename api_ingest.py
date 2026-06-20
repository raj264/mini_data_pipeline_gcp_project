"""Module: api_ingest.py
Handles ingestion from REST, SOAP, GraphQL, and gRPC APIs."""
import json
import logging
import time

import grpc
import requests
from google.cloud import storage
from zeep import Client

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
BACKOFF_SECONDS = 2


def _with_retries(description, func):
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return func()
        except Exception as e:
            last_error = e
            logger.error("%s failed (attempt %d/%d): %s", description, attempt, MAX_ATTEMPTS, e)
            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF_SECONDS * attempt)
    raise last_error


def fetch_rest_api_data(url: str, headers: dict = None, params: dict = None) -> dict:
    def _do():
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    return _with_retries(f"REST GET {url}", _do)


def fetch_soap_api_data(wsdl_url: str, method: str, **kwargs) -> dict:
    def _do():
        client = Client(wsdl_url)
        operation = getattr(client.service, method)
        result = operation(**kwargs)
        return json.loads(json.dumps(result))
    return _with_retries(f"SOAP {method}", _do)


def fetch_graphql_data(endpoint: str, query: str, variables: dict = None, headers: dict = None) -> dict:
    def _do():
        payload = {"query": query, "variables": variables or {}}
        response = requests.post(endpoint, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    return _with_retries(f"GraphQL {endpoint}", _do)


def fetch_grpc_data(target: str, stub_class, request) -> dict:
    def _do():
        channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())
        stub = stub_class(channel)
        resp = stub.MyMethod(request)
        return json.loads(json.dumps(resp, default=lambda x: x.__dict__))
    return _with_retries(f"gRPC {target}", _do)


def upload_json_to_gcs(data: dict, bucket_name: str, blob_name: str) -> None:
    def _do():
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(data), content_type='application/json')
    _with_retries(f"GCS upload {bucket_name}/{blob_name}", _do)
