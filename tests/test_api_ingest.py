from unittest.mock import MagicMock, patch

import pytest
import requests

from api_ingest import fetch_rest_api_data, fetch_graphql_data, upload_json_to_gcs


def test_fetch_rest_api_data_returns_json():
    with patch("api_ingest.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_get.return_value = mock_response

        result = fetch_rest_api_data("https://api.example.com/data")

        assert result == {"data": []}
        mock_response.raise_for_status.assert_called_once()


def test_fetch_rest_api_data_retries_on_failure_then_raises():
    with patch("api_ingest.requests.get", side_effect=requests.ConnectionError("boom")), \
         patch("api_ingest.time.sleep", return_value=None):
        with pytest.raises(requests.ConnectionError):
            fetch_rest_api_data("https://api.example.com/data")


def test_fetch_graphql_data_posts_query():
    with patch("api_ingest.requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {"ok": True}}
        mock_post.return_value = mock_response

        result = fetch_graphql_data("https://api.example.com/graphql", "{ ok }")

        assert result == {"data": {"ok": True}}


def test_upload_json_to_gcs_uploads_serialized_data():
    with patch("api_ingest.storage.Client") as mock_client_class:
        mock_blob = MagicMock()
        mock_client_class.return_value.bucket.return_value.blob.return_value = mock_blob

        upload_json_to_gcs({"id": 1}, "my-bucket", "raw/data.json")

        mock_blob.upload_from_string.assert_called_once()
