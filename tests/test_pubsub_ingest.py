import base64
from unittest.mock import MagicMock, patch

import pytest

from pubsub_ingest import pubsub_handler


def test_pubsub_handler_uploads_decoded_data():
    with patch("pubsub_ingest.storage.Client") as mock_client_class:
        mock_blob = MagicMock()
        mock_client_class.return_value.bucket.return_value.blob.return_value = mock_blob

        event = {"data": base64.b64encode(b'{"id": 1}').decode()}
        context = MagicMock(event_timestamp="2024-01-01T00:00:00Z")

        with patch.dict("os.environ", {"RAW_BUCKET": "raw-bucket"}):
            result = pubsub_handler(event, context)

        mock_blob.upload_from_string.assert_called_once_with(b'{"id": 1}')
        assert "2024-01-01T00:00:00Z" in result


def test_pubsub_handler_rejects_missing_data_field():
    with pytest.raises(ValueError):
        pubsub_handler({}, MagicMock())


def test_pubsub_handler_rejects_invalid_base64():
    with pytest.raises(ValueError):
        pubsub_handler({"data": "not-valid-base64!!"}, MagicMock())
