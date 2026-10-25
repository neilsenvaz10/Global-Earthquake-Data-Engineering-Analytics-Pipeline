"""Counters reconcile validation, filtering, deduplication and staging changes."""
from datetime import datetime, timezone

COUNTERS = ("fetched", "accepted", "rejected", "filtered", "eligible", "deduplicated",
            "unique_records", "inserted", "updated", "unchanged")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def empty_counts():
    return dict.fromkeys(COUNTERS, 0)


def reconcile(counts):
    if (counts["fetched"] != counts["accepted"] + counts["rejected"]
        or counts["accepted"] != counts["eligible"] + counts["filtered"]
        or counts["eligible"] != counts["deduplicated"] + counts["unique_records"]
        or counts["unique_records"] != counts["inserted"] + counts["updated"] + counts["unchanged"]):
        raise ValueError("ETL counters did not reconcile")
