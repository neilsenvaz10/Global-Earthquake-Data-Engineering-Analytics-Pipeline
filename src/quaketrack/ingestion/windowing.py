"""Millisecond-aligned, half-open UTC windows [start, end)."""
from datetime import datetime, timedelta, timezone

MILLISECOND = timedelta(milliseconds=1)


def parse_utc(value: str | datetime) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    result = result.replace(tzinfo=result.tzinfo or timezone.utc).astimezone(timezone.utc)
    if result.microsecond % 1000:
        raise ValueError("Time boundaries must align to milliseconds")
    return result


def windows(start: datetime, end: datetime, size: timedelta):
    start, end = parse_utc(start), parse_utc(end)
    if start >= end or size < MILLISECOND or size % MILLISECOND:
        raise ValueError("Require start < end and a positive millisecond-aligned window size")
    while start < end:
        stop = min(start + size, end)
        yield start, stop
        start = stop


def split_window(start: datetime, end: datetime):
    ticks = (end - start) // MILLISECOND
    if ticks < 2:
        raise ValueError("USGS result cap reached in a one-millisecond window; cannot guarantee completeness")
    middle = start + (ticks // 2) * MILLISECOND
    return (start, middle), (middle, end)


def source_times(start: datetime, end: datetime) -> dict:
    # USGS bounds are inclusive; events use epoch milliseconds.
    # Use documented implicit-UTC strings at the API boundary. Keep explicit UTC
    # offsets in normalized data, checkpoints and reports.
    def format_time(value):
        utc = value.astimezone(timezone.utc).replace(tzinfo=None)
        if utc.microsecond:
            return utc.isoformat(timespec="milliseconds").rstrip("0").rstrip(".")
        return utc.isoformat(timespec="seconds")
    return {"starttime": format_time(start), "endtime": format_time(end - MILLISECOND)}
