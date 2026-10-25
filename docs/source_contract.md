# Source and normalized contract v1

Source: United States Geological Survey, Earthquake Catalog FDSN Event API.
References checked 2026-10-25:
- https://earthquake.usgs.gov/fdsnws/event/1/
- https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
- https://www.usgs.gov/information-policies-and-instructions/copyrights-and-credits

## Connectivity sample

`check_api.py` requests format=geojson, starttime, endtime, minmagnitude=2.5,
eventtype=earthquake, orderby=time-asc, limit=10. Defaults select 2026-10-01
through 2026-10-02 UTC. This is a capped demonstration, not a complete day's data.
The raw response is saved byte-for-byte under a unique run directory with a sidecar
containing fetch time in UTC, request URL/parameters, record count, SHA-256 and
inspection summary. No normalization or loading occurs. Raw samples are ignored by
Git; retain the paired files locally for evidence. No authentication is required.

USGS query results are limited to 20,000 events; oversized queries can return 400.
Offsets are one-based. Start and end bounds are inclusive, so Phase 2 must explicitly
resolve adjacent-window boundaries. ISO8601 times without a zone mean UTC. Narrow
windows are preferred for historical extraction. USGS recommends real-time GeoJSON
feeds for applications displaying current events; this project uses catalog queries
for historical and revision-aware analytics. No undocumented request-rate quota is
assumed. Bounded retries, backoff and Retry-After support are Phase 2 work.

## Normalized record v1 (implemented in Phase 2)

| Field | Type / source / rule |
|---|---|
| event_id | nonempty text; feature.id; logical identity |
| occurred_at | aware UTC datetime; properties.time epoch milliseconds; required |
| source_updated_at | nullable aware UTC datetime; properties.updated epoch milliseconds |
| magnitude | nullable finite float; properties.mag; retain unknown |
| longitude | required finite float [-180,180]; coordinates[0] |
| latitude | required finite float [-90,90]; coordinates[1] |
| depth_km | nullable finite float; coordinates[2]; preserve legitimate negative depth |
| place_description | nullable text; properties.place; not a verified country |
| event_type | text; properties.type; earthquake-only curated policy |
| source_url | nullable text; properties.url |

GeoJSON coordinate order is longitude, latitude, depth (km). Do not invent values
for missing fields. Occurrence, source update and ingestion timestamps are distinct.
Invalid IDs/times/coordinates are quarantined in Phase 2; duplicate IDs prefer
the newest valid source revision under the policy in etl_operations.md. Storage provenance fields and migrations belong to Phase 3.

USGS-produced information is generally public domain in the United States; linked
third-party materials can carry separate rights. Attribute USGS, preserve source URLs
and review the linked policy before redistributing material. Earthquake catalog
coverage and revisions affect counts; this is educational analytics, not prediction.

## Observed live sample (2026-10-25)

Ten real events were archived; null magnitudes=0, null depths=0, event types=earthquake.
This sample does not demonstrate that nulls never occur; offline mocks cover them.
First source ID: ci41341279; magnitude 3.08; coordinates
[-116.463333333333, 33.5023333333333, 5.13] (longitude, latitude, depth km).
Occurrence: 2026-10-01T00:01:59.630000+00:00.
Source update: 2026-10-21T00:56:54.040000+00:00.
These were inspected directly from the archived source, without loading curated rows.

## Phase 2 extensions

Normalized records also retain nullable magnitude_type (magType), event_status
(status), network (net), source_code (code), source_ids (ids), source_networks
(sources), detail_url (detail), and the source label USGS. Event type is required.
Optional null measurements are retained; malformed present values are quarantined.
Normalized timestamps are UTC ISO8601 strings for JSON staging; the future loader
will bind them to PostgreSQL TIMESTAMPTZ. See etl_operations.md for revision ties,
half-open windows, adaptive extraction limits and count definitions. The old
Phase 1 sample checker remains capped and separate from complete-window ETL.
