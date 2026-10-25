"""Small synchronous client with bounded retries; HTTP payloads remain unmodified."""
import logging
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

log = logging.getLogger(__name__)
RETRYABLE = {408, 429, 500, 502, 503, 504}


def retry_delay(value: str | None, attempt: int, base: float) -> float:
    delay = base * (2 ** attempt) + random.uniform(0, base)
    if value:
        try:
            requested = float(value)
        except ValueError:
            try:
                requested = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                requested = 0
        delay = max(delay, requested)
    return delay


class USGSClient:
    def __init__(self, settings, *, retries=3, backoff=1.0, sleep=time.sleep):
        self.settings = settings
        self.retries = retries
        self.backoff = backoff
        self.sleep = sleep
        self.attempts = 0
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "QuakeTrack/0.2 educational historical ETL"

    def close(self):
        self.session.close()

    def get(self, params: dict, archive):
        """Archive every received response, including failures, before interpretation."""
        for attempt in range(self.retries + 1):
            self.attempts += 1
            retry_after = None
            try:
                response = self.session.get(self.settings.api_url, params=params,
                                            timeout=self.settings.timeout)
                reference = archive(response, params, attempt + 1)
                if response.status_code not in RETRYABLE:
                    response.raise_for_status()
                    return response, reference
                retry_after = response.headers.get("Retry-After")
                response.raise_for_status()
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                if isinstance(exc, requests.HTTPError) and exc.response.status_code not in RETRYABLE:
                    raise
                if attempt == self.retries:
                    raise
                delay = retry_delay(retry_after, attempt, self.backoff)
                # Do not violate long server-directed waits or hang an interactive run.
                if delay > 300:
                    raise RuntimeError("Retry-After exceeds five minutes; retry the run later") from exc
                log.warning("USGS attempt %s failed (%s); retry in %.2fs", attempt + 1,
                            type(exc).__name__, delay)
                self.sleep(delay)
        raise RuntimeError("Unreachable retry state")


def parse_response(response) -> list:
    if response.status_code == 204:
        return []
    def reject_constant(value):
        raise ValueError(f"Nonstandard JSON numeric constant: {value}")
    payload = response.json(parse_constant=reject_constant)
    if not isinstance(payload, dict) or payload.get("type") != "FeatureCollection" or not isinstance(payload.get("features"), list):
        raise ValueError("Expected a GeoJSON FeatureCollection with a features list")
    features = payload["features"]
    metadata = payload.get("metadata", {})
    if not isinstance(metadata, dict):
        raise ValueError("USGS metadata must be an object")
    count = metadata.get("count")
    if count is not None and (isinstance(count, bool) or not isinstance(count, int) or count != len(features)):
        raise ValueError("USGS metadata count does not match returned features")
    return features
