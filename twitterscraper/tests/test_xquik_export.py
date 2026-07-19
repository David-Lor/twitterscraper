import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "services" / "xquik_export.py"
SPEC = importlib.util.spec_from_file_location("xquik_export", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
xquik_export = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(xquik_export)
normalize_xquik_export = xquik_export.normalize_xquik_export
parse_xquik_export = xquik_export.parse_xquik_export


def test_parse_xquik_json_export():
    records = parse_xquik_export('{"tweets": [{"id": "1", "text": " First tweet "}]}')

    assert records == [{"id": "1", "text": " First tweet "}]


def test_normalize_xquik_jsonl_export():
    tweets = normalize_xquik_export(
        '{"id":"1","full_text":"First","created_at":"2026-01-01T00:00:00Z"}\n'
        '{"url":"https://x.com/acme/status/2","tweet":"Second","conversation_id":"1"}',
        "tweets.jsonl",
    )

    assert tweets == [
        {"tweet_id": "1", "text": "First", "timestamp": 1767225600, "is_reply": False},
        {"tweet_id": "2", "text": "Second", "timestamp": 1, "is_reply": True},
    ]


def test_parse_xquik_csv_export():
    tweets = normalize_xquik_export("id,content,timestamp\n1,Needs review,1767225600\n", "tweets.csv")

    assert tweets == [{"tweet_id": "1", "text": "Needs review", "timestamp": 1767225600, "is_reply": False}]


def test_normalize_xquik_tweet_text_csv_header():
    tweets = normalize_xquik_export(
        "Tweet ID,Tweet Text,Created-At\n1,Needs review,2026-01-01T00:00:00Z\n",
        "tweets.csv",
    )

    assert tweets == [
        {
            "tweet_id": "1",
            "text": "Needs review",
            "timestamp": 1767225600,
            "is_reply": False,
        }
    ]


def test_non_finite_timestamp_falls_back():
    tweets = normalize_xquik_export(
        '[{"id":"1","text":"Needs review","timestamp":"Infinity"}]'
    )

    assert tweets == [
        {"tweet_id": "1", "text": "Needs review", "timestamp": 1, "is_reply": False}
    ]


def test_reject_csv_without_text_column():
    try:
        parse_xquik_export("id,url\n1,https://example.com\n", "tweets.csv")
    except ValueError as exc:
        assert "CSV export needs" in str(exc)
    else:
        raise AssertionError("expected missing text column to fail")
