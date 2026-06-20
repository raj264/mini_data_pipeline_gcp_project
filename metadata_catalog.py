"""Module: metadata_catalog.py
Manage Data Catalog entries and IAM policies."""
from google.cloud import datacatalog_v1

def create_datacatalog_entry(project_id: str, location: str, entry_group_id: str, entry_id: str, gcs_uri: str):
    """Register a GCS path or BigQuery table in Data Catalog."""
    client = datacatalog_v1.DataCatalogClient()
    parent = client.entry_group_path(project_id, location, entry_group_id)
    entry = datacatalog_v1.Entry()
    entry.display_name = entry_id
    entry.gcs_fileset_spec = datacatalog_v1.GcsFilesetSpec(file_patterns=[gcs_uri])
    return client.create_entry(parent=parent, entry_id=entry_id, entry=entry)

def set_iam_policy(resource: str, member: str, role: str):
    """Attach IAM policy to a resource."""
    client = datacatalog_v1.DataCatalogClient()
    policy = client.get_policy(resource=resource)
    policy.bindings.add(role=role, members=[member])
    client.set_policy(resource=resource, policy=policy)
