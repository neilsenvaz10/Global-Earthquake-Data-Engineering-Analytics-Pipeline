"""Small presentation helpers; CSS targets only our markup or documented keys."""
from datetime import datetime, timedelta, timezone
from html import escape
import json

import streamlit as st

from ..config import ROOT
from .data import utc_label, freshness

CSS = """<style>
.st-key-qt_shell{margin-top:-2rem;container-type:inline-size}
.qt-hero{display:flex;justify-content:space-between;align-items:center;gap:20px;margin:0 0 4px}
.qt-identity{display:flex;gap:14px;align-items:center}.qt-mark{color:#2dd4bf;font-size:30px;background:#122f38;border:1px solid #245960;border-radius:12px;padding:7px 12px}
.qt-title{font-size:30px;font-weight:750;letter-spacing:-1px;line-height:1.1;color:#f1f6fc}.qt-subtitle{font-size:13px;color:#a4b5cb;margin-top:6px}
.qt-eyebrow{font-size:10px;font-weight:650;letter-spacing:2px;text-transform:uppercase;color:#53d6ca;margin-bottom:7px}
.qt-status{text-align:right;font-size:12px;color:#a4b5cb}.qt-pill{display:inline-block;border:1px solid #245960;background:#102d32;color:#6fe0cf;border-radius:20px;padding:4px 10px;font-size:11px;margin-bottom:5px}
.qt-section{display:flex;justify-content:space-between;align-items:baseline;margin:4px 0 6px}.qt-section h2{font-size:22px;margin:0;letter-spacing:-.4px}.qt-muted{font-size:12px;color:#a4b5cb}
.qt-scope{border:1px solid #39475c;border-left:3px solid #f6b95e;border-radius:9px;padding:8px 12px;font-size:12px;line-height:1.6;background:#132033;color:#c6d3e3;margin-bottom:4px}
.qt-scope strong{color:#f6c978}.qt-filterbar{font-size:12px;color:#9db0ca;margin-bottom:4px;line-height:1.6}
div[class*="st-key-kpi_"]{background:#111f32;border-top:2px solid #28bfb2;border-radius:12px}
.qt-brand{font-weight:750;font-size:23px;color:#eff5fd;letter-spacing:-.7px}.qt-brand small{display:block;font-size:10px;letter-spacing:2px;color:#53d6ca;margin:5px 0 12px}
.qt-kpis{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin:0 0 10px}.qt-kpi{background:#111f32;border:1px solid #26384e;border-top:2px solid #2dbfaf;border-radius:11px;padding:17px 18px;min-width:0}.qt-kpi-label{color:#a9bbd0;font-size:12px;margin-bottom:10px}.qt-kpi-value{font-size:30px;color:#eff6ff;font-weight:650;letter-spacing:-.8px}.qt-kpi-note{color:#849bb7;font-size:11px;margin-top:6px}
@media(max-width:1100px){.qt-kpis{grid-template-columns:repeat(3,minmax(0,1fr))}}
@container(max-width:1100px){.qt-kpis{grid-template-columns:repeat(3,minmax(0,1fr))}}
@container(max-width:650px){.qt-kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.qt-kpi-value{font-size:25px}}
@media(max-width:650px){.qt-kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.qt-kpi-value{font-size:25px}.qt-section{align-items:flex-start;flex-direction:column;gap:5px}}
@media(max-width:800px){.qt-hero{align-items:flex-start;flex-direction:column;gap:10px}.qt-status{text-align:left}.qt-title{font-size:26px}.qt-section h2{font-size:20px}}
</style>"""


def header(coverage):
    loaded = coverage.get("last_successful_load") or coverage.get("last_successful_workflow")
    st.html(f'''<div class="qt-hero"><div class="qt-identity"><div class="qt-mark">⌁</div><div>
        <div class="qt-eyebrow">USGS observations · Data engineering</div><div class="qt-title">QuakeTrack</div>
        <div class="qt-subtitle">Global Earthquake Intelligence</div></div></div>
        <div class="qt-status"><span class="qt-pill">POSTGRESQL · READ ONLY</span><br>
        Last successful DB load · {escape(freshness(loaded))}<br>{loaded.strftime('%d %b %Y · %H:%M UTC') if loaded else 'No successful load recorded'}</div></div>''')


def section(title, subtitle=""):
    st.html(f'<div class="qt-section"><h2>{escape(title)}</h2><span class="qt-muted">{escape(subtitle)}</span></div>')


def kpis(summary):
    def value(key, suffix=""):
        return "Unknown" if summary[key] is None else f"{summary[key]:.2f}{suffix}"
    latest = summary["latest_event"]
    items = [("Recorded earthquakes", f"{summary['total']:,}", "Matching your filters"),
        ("Highest magnitude", value("max_magnitude"), "Reported source magnitude"),
        ("Average magnitude", value("avg_magnitude"), "Known measurements only"),
        ("Average depth", value("avg_depth", " km"), "Known measurements only"),
        ("Latest earthquake · UTC", latest.strftime("%d %b %Y") if latest else "Unknown",
         latest.strftime("%H:%M:%S UTC") if latest else "No event timestamp")]
    cards = "".join(f'<div class="qt-kpi"><div class="qt-kpi-label">{escape(label)}</div><div class="qt-kpi-value">{escape(number)}</div><div class="qt-kpi-note">{escape(note)}</div></div>' for label,number,note in items)
    st.html('<div class="qt-kpis">' + cards + '</div>')


def coverage_note(coverage):
    if not coverage["total"]:
        st.info("The database is connected but contains no earthquakes. Run a bounded ingestion to populate it.")
        return
    thresholds = ", ".join(str(row["minimum_magnitude"] or "unspecified") for row in coverage["source_filters"])
    st.html(f'''<div class="qt-scope"><strong>Catalog scope</strong> · {coverage['total']:,} stored events ·
        {coverage['first_event'].strftime('%d %b %Y')} → {coverage['last_event'].strftime('%d %b %Y')} UTC ·
        {coverage['days']:,} event dates · source minimum magnitudes {escape(thresholds)}.<br>
        Ingested observations, not comprehensive global history. Event dates alone do not prove complete ingestion coverage.</div>''')


def complete_days(windows, filters):
    """Count whole UTC days in merged successful windows, separately by threshold."""
    groups = {}
    for row in windows:
        if row.get("event_type") != "earthquake":
            continue
        start = max(row["window_start"].astimezone(timezone.utc), filters.start.astimezone(timezone.utc))
        end = min(row["window_end"].astimezone(timezone.utc), filters.end.astimezone(timezone.utc))
        if start < end:
            groups.setdefault(str(row.get("minimum_magnitude") or "unspecified"), []).append((start, end))
    result = {}
    for threshold, intervals in groups.items():
        merged = []
        for start, end in sorted(intervals):
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
            else:
                merged.append((start, end))
        count = 0
        for start, end in merged:
            midnight = start.replace(hour=0, minute=0, second=0, microsecond=0)
            left = midnight if start == midnight else midnight + timedelta(days=1)
            right = end.replace(hour=0, minute=0, second=0, microsecond=0)
            count += max(0, (right - left).days)
        result[threshold] = count
    return result


def period_coverage(rows, windows, filters, interval):
    """Flag whole calendar periods covered by successful, sufficiently broad loads."""
    eligible = []
    for row in windows:
        try:
            threshold = float(row["minimum_magnitude"])
        except (TypeError, ValueError, KeyError):
            continue
        if row.get("event_type") == "earthquake":
            eligible.append((threshold,row["window_start"].astimezone(timezone.utc),row["window_end"].astimezone(timezone.utc)))
    minimum = filters.min_magnitude if filters.min_magnitude is not None else min((row[0] for row in eligible),default=float("-inf"))
    merged = []
    for left,right in sorted((left,right) for threshold,left,right in eligible if threshold <= minimum):
        if merged and left <= merged[-1][1]:
            merged[-1] = (merged[-1][0],max(right,merged[-1][1]))
        else:
            merged.append((left,right))
    result = []
    for row in rows:
        left = row["period"]
        left = left.replace(tzinfo=timezone.utc) if left.tzinfo is None else left.astimezone(timezone.utc)
        if interval == "month":
            right = left.replace(year=left.year+(left.month==12),month=1 if left.month==12 else left.month+1)
        else:
            right = left + timedelta(days=7 if interval=="week" else 1)
        result.append(left >= filters.start and right <= filters.end and any(a<=left and b>=right for a,b in merged))
    return result


def saved_validation(coverage, path=None):
    """Historical checks are never presented as live checks or silently fabricated."""
    path = path or ROOT / "data/staging/phase7/validation/summary.json"
    if not path.exists():
        return None
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
        expected = report["dataset"]
        current = dict(total=coverage["total"], last_ingested_at=utc_label(coverage.get("last_ingested_at")),
                       latest_source_update=utc_label(coverage.get("latest_source_update")))
        return {**report, "matches_current_summary": expected == current}
    except (ValueError, OSError, KeyError, TypeError):
        return None
