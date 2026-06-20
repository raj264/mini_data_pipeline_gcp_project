import json

import apache_beam as beam

from validation import ClassifyRecordDoFn


def _run(dofn, element):
    return list(dofn.process(element))


def test_valid_record_yields_main_output():
    dofn = ClassifyRecordDoFn(['id', 'email'])
    record = json.dumps({"id": 1, "email": "a@b.com"})

    results = _run(dofn, record)

    assert results == [record]


def test_missing_required_field_yields_tagged_invalid():
    dofn = ClassifyRecordDoFn(['id', 'email'])
    record = json.dumps({"id": 1})

    results = _run(dofn, record)

    assert len(results) == 1
    assert isinstance(results[0], beam.pvalue.TaggedOutput)
    assert results[0].tag == ClassifyRecordDoFn.OUTPUT_TAG_INVALID


def test_bad_email_format_yields_tagged_invalid():
    dofn = ClassifyRecordDoFn(['id', 'email'])
    record = json.dumps({"id": 1, "email": "not-an-email"})

    results = _run(dofn, record)

    assert isinstance(results[0], beam.pvalue.TaggedOutput)


def test_unparseable_json_yields_tagged_invalid():
    dofn = ClassifyRecordDoFn(['id', 'email'])

    results = _run(dofn, "not json")

    assert isinstance(results[0], beam.pvalue.TaggedOutput)
