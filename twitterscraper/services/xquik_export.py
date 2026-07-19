import csv
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


TEXT_FIELDS = ("text", "tweet", "tweet text", "full_text", "content", "body")
ID_FIELDS = ("id", "tweet_id", "id_str", "post_id")
TIME_FIELDS = ("created_at", "published_at", "timestamp", "time")
URL_FIELDS = ("url", "tweet_url", "permalink")


def parse_xquik_export(raw_export: str, filename: str = "tweets.json") -> List[Dict[str, Any]]:
    if not raw_export.strip():
        return []

    lowered_name = filename.lower()
    if lowered_name.endswith(".csv"):
        return _parse_csv(raw_export)
    if lowered_name.endswith(".jsonl"):
        return _parse_jsonl(raw_export)

    try:
        parsed = json.loads(raw_export)
    except json.JSONDecodeError as exc:
        if lowered_name.endswith(".json"):
            raise ValueError("Xquik JSON export contains invalid JSON.") from exc
        return _parse_jsonl(raw_export)

    return _records_from_json(parsed)


def normalize_xquik_record(record: Dict[str, Any]) -> Dict[str, Any]:
    text = _first_text(record, TEXT_FIELDS)
    tweet_id = _first_text(record, ID_FIELDS) or _status_id_from_url(_first_text(record, URL_FIELDS))
    timestamp = _timestamp_from_value(_first_text(record, TIME_FIELDS))
    if not text or not tweet_id:
        return {}
    return {
        "tweet_id": str(tweet_id),
        "text": text,
        "timestamp": timestamp,
        "is_reply": _is_reply(record, str(tweet_id)),
    }


def normalize_xquik_export(raw_export: str, filename: str = "tweets.json") -> List[Dict[str, Any]]:
    normalized = [normalize_xquik_record(record) for record in parse_xquik_export(raw_export, filename)]
    return [record for record in normalized if record]


def _parse_csv(raw_export: str) -> List[Dict[str, Any]]:
    reader = csv.DictReader(raw_export.splitlines())
    if reader.fieldnames is None:
        return []
    if _find_field(reader.fieldnames, TEXT_FIELDS) is None:
        raise ValueError("Xquik CSV export needs a text, tweet, full_text, content, or body column.")
    return [dict(row) for row in reader]


def _parse_jsonl(raw_export: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for line in raw_export.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError("Xquik JSONL export contains an invalid JSON line.") from exc
        if isinstance(parsed, dict):
            records.append(parsed)
    return records


def _records_from_json(parsed: Any) -> List[Dict[str, Any]]:
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        for key in ("tweets", "items", "data", "results"):
            value = parsed.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [parsed]
    return []


def _first_text(record: Dict[str, Any], fields: tuple) -> str:
    field = _find_field(record.keys(), fields)
    if field is None:
        return ""
    value = record.get(field)
    if value is None:
        return ""
    return " ".join(str(value).split())


def _find_field(fields: Any, candidates: tuple) -> Optional[str]:
    normalized_fields = {
        _normalize_field_name(field): str(field) for field in fields
    }
    for candidate in candidates:
        normalized_candidate = _normalize_field_name(candidate)
        if normalized_candidate in normalized_fields:
            return normalized_fields[normalized_candidate]
    return None


def _normalize_field_name(field: Any) -> str:
    return " ".join(
        str(field).replace("_", " ").replace("-", " ").lower().split()
    )


def _timestamp_from_value(value: str) -> int:
    if not value:
        return 1
    try:
        return int(float(value))
    except (OverflowError, ValueError):
        pass
    normalized = value.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(normalized).timestamp())
    except ValueError:
        return 1


def _status_id_from_url(url: str) -> str:
    match = re.search(r"/status/(\d+)", url)
    return match.group(1) if match else ""


def _is_reply(record: Dict[str, Any], tweet_id: str) -> bool:
    reply_to = _first_text(record, ("in_reply_to_status_id", "in_reply_to_tweet_id", "reply_to_id"))
    if reply_to:
        return True
    conversation_id = _first_text(record, ("conversation_id",))
    return bool(conversation_id and conversation_id != tweet_id)
