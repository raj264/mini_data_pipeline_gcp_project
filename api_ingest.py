"""Module: api_ingest.py
Handles ingestion from REST, SOAP, GraphQL, and gRPC APIs."""
import requests
import json
from google.cloud import storage
from zeep import Client
import grpc

def fetch_rest_api_data(url: str, headers: dict = None, params: dict = None) -> dict:
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def fetch_soap_api_data(wsdl_url: str, method: str, **kwargs) -> dict:
    client = Client(wsdl_url)
    operation = getattr(client.service, method)
    result = operation(**kwargs)
    return json.loads(json.dumps(result))

def fetch_graphql_data(endpoint: str, query: str, variables: dict = None, headers: dict = None) -> dict:
    payload = {"query": query, "variables": variables or {}}
    response = requests.post(endpoint, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_grpc_data(target: str, stub_class, request) -> dict:
    channel = grpc.insecure_channel(target)
    stub = stub_class(channel)
    resp = stub.MyMethod(request)
    return json.loads(json.dumps(resp, default=lambda x: x.__dict__))

def upload_json_to_gcs(data: dict, bucket_name: str, blob_name: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(data), content_type='application/json')
