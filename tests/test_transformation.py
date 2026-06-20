from transformation import EnrichDoFn


def _run(dofn, element):
    return list(dofn.process(element))


def test_process_casts_id_and_derives_year_month():
    dofn = EnrichDoFn("project.dataset.lookup")
    dofn._lookup_by_id = {}

    result = _run(dofn, {"id": 1, "timestamp": "2024-03-15T10:00:00"})

    assert len(result) == 1
    record = result[0]
    assert record["id"] == "1"
    assert record["year"] == 2024
    assert record["month"] == 3


def test_process_enriches_with_lookup_match():
    dofn = EnrichDoFn("project.dataset.lookup")
    dofn._lookup_by_id = {"1": {"region": "us-east"}}

    result = _run(dofn, {"id": 1, "timestamp": "2024-03-15T10:00:00"})

    assert result[0]["region"] == "us-east"


def test_process_skips_record_with_invalid_timestamp():
    dofn = EnrichDoFn("project.dataset.lookup")
    dofn._lookup_by_id = {}

    result = _run(dofn, {"id": 1, "timestamp": "not-a-timestamp"})

    assert result == []


def test_process_skips_record_missing_timestamp():
    dofn = EnrichDoFn("project.dataset.lookup")
    dofn._lookup_by_id = {}

    result = _run(dofn, {"id": 1})

    assert result == []
