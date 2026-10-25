"""A processed-record contract, reusing existing field and coordinate validators."""
from collections import Counter
from datetime import datetime, timedelta
import json
from uuid import UUID

from .config import ROOT
from .ingestion.windowing import parse_utc
from .transforms.validator import coordinates, number, text, ValidationError

CONTRACT_PATH = ROOT / "contracts/earthquake.v1.json"


def load_contract():
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def validate_contract(record, *, require_provenance=False):
    """Return errors without repairing, deduplicating or mutating supplied data.

    Unlike raw-source normalization, processed numeric fields must already be numbers
    and timestamps must already identify UTC explicitly. The domain rules are shared.
    """
    if not isinstance(record, dict):
        return [{"field": "record", "code": "type", "reason": "Expected a processed object"}]
    errors = []
    for field, rule in load_contract()["fields"].items():
        value = record.get(field)
        if value is None or (rule["required"] and isinstance(value, str) and not value.strip()):
            if rule["required"]:
                errors.append(dict(field=field, code="required", reason="Required value is missing"))
            continue
        try:
            if rule["type"] == "number":
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValidationError(field, "Processed numbers must be numeric, not strings or booleans")
                number(value, field, required=rule["required"])
                if field == "longitude":
                    coordinates(value, 0)
                elif field == "latitude":
                    coordinates(0, value)
            elif rule["type"] == "string":
                text(value, field, required=rule["required"])
                if "constant" in rule and value != rule["constant"]:
                    raise ValidationError(field, "Unexpected source")
            else:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
                if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
                    raise ValidationError(field, "Expected an explicit UTC ISO8601 timestamp")
                parse_utc(parsed)  # Existing millisecond precision/range policy.
        except (ValueError, TypeError, AttributeError, OverflowError) as error:
            errors.append(dict(field=field, code="coordinate" if field in ("latitude", "longitude") else "invalid",
                               reason=error.reason if isinstance(error, ValidationError) else "Invalid field type, timestamp or precision"))
    if require_provenance:
        try:
            UUID(str(record["last_run_id"]))
        except (KeyError, ValueError, TypeError, AttributeError):
            errors.append(dict(field="last_run_id", code="provenance", reason="A valid persisted ingestion run UUID is required"))
    return errors


def quality_report(records, *, require_provenance=False):
    records = list(records)
    counts = Counter(row["event_id"] for row in records if isinstance(row, dict)
                     and isinstance(row.get("event_id"), str) and row["event_id"].strip())
    duplicate_ids = sorted(key for key, count in counts.items() if count > 1)
    failures, missing, coordinates_failed = [], 0, 0
    for index, record in enumerate(records):
        errors = validate_contract(record, require_provenance=require_provenance)
        missing += sum(error["code"] == "required" for error in errors)
        coordinates_failed += any(error["code"] == "coordinate" for error in errors)
        event_id = record.get("event_id") if isinstance(record, dict) else None
        if isinstance(event_id, str) and event_id in duplicate_ids:
            errors.append(dict(field="event_id", code="duplicate", reason="Repeated ID in supplied Silver dataset"))
        if errors:
            failures.append(dict(row_index=index, errors=errors))
    total = len(records)
    return dict(contract=load_contract()["name"], version=load_contract()["version"],
        input_stage="processed Silver rows", denominator="all supplied processed rows, including duplicates",
        total_records_checked=total, valid_records=total - len(failures), invalid_records=len(failures),
        duplicate_event_ids=len(duplicate_ids), duplicate_records=sum(counts[key] - 1 for key in duplicate_ids),
        missing_required_values=missing, coordinate_validation_failures=coordinates_failed,
        contract_compliance_percentage=round(100 * (total - len(failures)) / total, 4) if total else None,
        require_provenance=require_provenance, failures=failures)
