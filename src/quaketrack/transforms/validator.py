"""Explicit required and optional field validation; no imputed measurements."""
import math
from datetime import datetime, timedelta, timezone


class ValidationError(ValueError):
    def __init__(self, field, reason):
        self.field = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


def number(value, field, *, required=False):
    if value is None:
        if required:
            raise ValidationError(field, "required")
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValidationError(field, "must be numeric")
    try:
        result = float(value)
    except (ValueError, OverflowError):
        raise ValidationError(field, "must be numeric") from None
    if not math.isfinite(result):
        raise ValidationError(field, "must be finite")
    return result


def timestamp(value, field, *, required=False):
    value = number(value, field, required=required)
    if value is None:
        return None
    if not value.is_integer():
        raise ValidationError(field, "epoch milliseconds must be integral")
    try:
        return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=value)).isoformat(timespec="milliseconds")
    except (OverflowError, ValueError):
        raise ValidationError(field, "timestamp is outside supported datetime range") from None


def text(value, field, *, required=False):
    if value is None and not required:
        return None
    if not isinstance(value, str) or (required and not value.strip()):
        raise ValidationError(field, "must be nonempty text" if required else "must be text or null")
    return value.strip()


def coordinates(longitude, latitude):
    if not -180 <= longitude <= 180:
        raise ValidationError("longitude", "outside [-180, 180]")
    if not -90 <= latitude <= 90:
        raise ValidationError("latitude", "outside [-90, 90]")
