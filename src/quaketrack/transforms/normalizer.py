"""Pure mapping from one source feature to a JSON-compatible UTC record."""
from .validator import ValidationError, coordinates, number, text, timestamp


def normalize_feature(feature) -> dict:
    if not isinstance(feature, dict) or feature.get("type") != "Feature":
        raise ValidationError("feature", "expected a GeoJSON Feature object")
    event_id = text(feature.get("id"), "event_id", required=True)
    props, geometry = feature.get("properties"), feature.get("geometry")
    if not isinstance(props, dict) or not isinstance(geometry, dict):
        raise ValidationError("feature", "properties and geometry must be objects")
    coords = geometry.get("coordinates")
    if geometry.get("type") != "Point" or not isinstance(coords, list) or len(coords) < 2:
        raise ValidationError("geometry", "expected Point with longitude and latitude")
    longitude = number(coords[0], "longitude", required=True)
    latitude = number(coords[1], "latitude", required=True)
    coordinates(longitude, latitude)
    return {
        "event_id": event_id,
        "occurred_at": timestamp(props.get("time"), "occurred_at", required=True),
        "source_updated_at": timestamp(props.get("updated"), "source_updated_at"),
        "magnitude": number(props.get("mag"), "magnitude"),
        "magnitude_type": text(props.get("magType"), "magnitude_type"),
        "depth_km": number(coords[2] if len(coords) > 2 else None, "depth_km"),
        "longitude": longitude, "latitude": latitude,
        "place_description": text(props.get("place"), "place_description"),
        "event_type": text(props.get("type"), "event_type", required=True),
        "event_status": text(props.get("status"), "event_status"),
        "source": "USGS", "network": text(props.get("net"), "network"),
        "source_code": text(props.get("code"), "source_code"),
        "source_ids": text(props.get("ids"), "source_ids"),
        "source_networks": text(props.get("sources"), "source_networks"),
        "source_url": text(props.get("url"), "source_url"),
        "detail_url": text(props.get("detail"), "detail_url"),
    }
