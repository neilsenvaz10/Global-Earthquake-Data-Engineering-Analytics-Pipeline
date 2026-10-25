"""Streamlit presentation. All five views share one validated filter definition."""
from datetime import datetime, timezone

import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from ..config import load_settings
from . import charts
from .data import CHART_LIMIT, EXPORT_LIMIT, DashboardData, Filters, dashboard_engine, freshness, utc_label

PAGES = ("Overview", "Earthquake Map", "Trends", "Explorer", "Pipeline Monitoring")


@st.cache_resource
def get_data():
    return DashboardData(dashboard_engine(load_settings()))


@st.cache_data(ttl=60, max_entries=256, show_spinner=False)
def cached(method, filters=None, _data=None, **kwargs):
    return getattr(_data, method)(**kwargs) if filters is None else getattr(_data, method)(filters, **kwargs)


def number(value, suffix=""):
    return "Unknown" if value is None else f"{value:.2f}{suffix}"


def utc_rows(rows):
    return [{key: utc_label(value) if isinstance(value, datetime) else value
             for key, value in row.items()} for row in rows]


def event_table(rows):
    # USGS links are supplied by the source; never construct countries or labels from place text.
    display = [{**row, "occurred_at": utc_label(row["occurred_at"])} for row in rows]
    st.dataframe(display, hide_index=True, width="stretch", column_config={
        "occurred_at": "Event time (UTC)", "depth_km": "Depth (km)",
        "source_url": st.column_config.LinkColumn("USGS event", display_text="Open USGS")})


def filters_ui(coverage):
    today = datetime.now(timezone.utc).date()
    first = coverage["first_event"].date() if coverage["first_event"] else today
    last = coverage["last_event"].date() if coverage["last_event"] else today
    with st.sidebar:
        st.subheader("Explore the dataset")
        start = st.date_input("Start date (UTC)", first, key="start")
        end = st.date_input("End date (UTC)", last, key="end")
        st.caption("Both dates included. All times are UTC.")
        st.checkbox("Filter magnitude", key="use_magnitude")
        min_mag = st.number_input("Minimum magnitude", value=float(coverage["min_magnitude"] or 0),
                                  disabled=not st.session_state.use_magnitude, key="min_mag")
        max_mag = st.number_input("Maximum magnitude", value=float(coverage["max_magnitude"] or 0),
                                  disabled=not st.session_state.use_magnitude, key="max_mag")
        st.checkbox("Filter depth", key="use_depth")
        min_depth = st.number_input("Minimum depth (km)", value=float(coverage["min_depth"] or 0),
                                    disabled=not st.session_state.use_depth, key="min_depth")
        max_depth = st.number_input("Maximum depth (km)", value=float(coverage["max_depth"] or 0),
                                    disabled=not st.session_state.use_depth, key="max_depth")
        location = st.text_input("Location contains", max_chars=200, key="location")
        unknown = st.checkbox("Include unknown magnitude / depth", value=True, key="unknown")
        st.caption("Unknown measurements are included by default, even when numeric bounds are active. "
                   "Uncheck to require both measurements. Negative values remain valid.")
    return Filters.dates(start, end, min_magnitude=min_mag if st.session_state.use_magnitude else None,
                         max_magnitude=max_mag if st.session_state.use_magnitude else None,
                         min_depth=min_depth if st.session_state.use_depth else None,
                         max_depth=max_depth if st.session_state.use_depth else None,
                         location=location, include_unknown=unknown)


def monitoring(data):
    st.subheader("Pipeline Monitoring")
    st.info("These are application database ingestion records, not live Airflow scheduler status. "
            "The DAG was left paused during deployment verification. Check Airflow for its current state.")
    st.link_button("Open Airflow · localhost:18080", "http://localhost:18080")
    st.caption("Operational history is global and independent of earthquake filters. "
               "Workflow parents and persistence loads are shown separately to avoid double-counting.")
    result = cached("monitoring", _data=data)
    workflows = result["workflows"]
    if not workflows:
        st.warning("No workflow runs have been recorded yet.")
    else:
        latest = workflows[0]
        a, b, c = st.columns(3)
        a.metric("Latest workflow status", latest["status"])
        b.metric("Duration (seconds)", number(latest["duration_seconds"]))
        c.metric("Quality", latest["quality_status"] or "Unknown")
        st.caption(f"Latest execution: {latest['execution_key']} · Started {utc_label(latest['started_at'])}")
        columns = st.columns(5)
        for column, label, key in zip(columns, ("Extracted", "Inserted", "Updated", "Unchanged", "Quarantined"),
            ("records_fetched", "records_inserted", "records_updated", "records_unchanged", "records_rejected")):
            column.metric(label, f"{latest[key]:,}")
        # error_message can contain operational detail; never send it to the browser.
        safe = [{k: v for k, v in row.items() if k not in ("error_message",)} for row in workflows]
        st.subheader("Recent workflow history · up to 50 runs")
        st.dataframe(utc_rows(safe), hide_index=True, width="stretch")
    st.write(f"Last successful workflow: **{utc_label(result['last_success'])}** · {freshness(result['last_success'])}")
    st.subheader("Incremental coverage checkpoints")
    st.dataframe(utc_rows(result["state"]), hide_index=True, width="stretch")
    st.caption("Checkpoint lag measures coverage progress; a recent successful commit does not imply complete historical coverage.")
    st.subheader("Persistence load history · up to 50 runs")
    st.dataframe(utc_rows(result["loads"]), hide_index=True, width="stretch")


def render(data):
    coverage = cached("coverage", _data=data)
    filters = filters_ui(coverage)
    page = st.session_state.page
    st.caption(f"SOURCE: USGS FDSN EVENT API  /  STORED EVENTS: {coverage['total']:,}  /  UTC")
    st.caption(f"Coverage queried at {utc_label(coverage.get('queried_at'))}; query results cached for up to 60 seconds.")
    if coverage["total"]:
        thresholds = "; ".join(f"min magnitude {r['minimum_magnitude'] or 'unspecified'}, type {r['event_type'] or 'unspecified'}"
                                for r in coverage["source_filters"]) or "No archive request metadata available"
        st.warning(f"Ingested sample, not comprehensive global history. Coverage: {utc_label(coverage['first_event'])} "
                   f"to {utc_label(coverage['last_event'])}, across {coverage['days']} event dates. Source requests: {thresholds}.")
    else:
        st.info("The database is connected but contains no earthquakes. Run a bounded ingestion to populate it.")
    if page == "Pipeline Monitoring":
        monitoring(data)
        return
    summary = cached("overview", filters, _data=data)
    st.subheader(page)
    st.caption(f"{summary['total']:,} matching events · {utc_label(filters.start)} ≤ event time < {utc_label(filters.end)}")
    st.caption(f"Active filters · magnitude {filters.min_magnitude if filters.min_magnitude is not None else 'any'} "
               f"to {filters.max_magnitude if filters.max_magnitude is not None else 'any'} · depth "
               f"{filters.min_depth if filters.min_depth is not None else 'any'} to "
               f"{filters.max_depth if filters.max_depth is not None else 'any'} km · location "
               f"{filters.location or 'any'} · unknown measurements {'included' if filters.include_unknown else 'excluded'}")
    if not summary["total"]:
        st.info("No events match these filters. Widen the date or measurement range, or clear location search.")
        return
    if page == "Overview":
        columns = st.columns(4)
        for column, label, value in zip(columns, ("Events", "Highest magnitude", "Average magnitude", "Average depth"),
            (f"{summary['total']:,}", number(summary["max_magnitude"]), number(summary["avg_magnitude"]), number(summary["avg_depth"], " km"))):
            column.metric(label, value)
        st.write(f"Latest event: **{utc_label(summary['latest_event'])}**")
        st.write(f"Most recent source update: **{utc_label(summary['latest_source_update'])}**")
        updated = cached("events", filters, _data=data, sort="Source update", limit=1)
        if updated and summary["latest_source_update"] is not None:
            st.caption(f"Most recently updated event: {updated[0]['event_id']} · "
                       f"{updated[0].get('place_description') or 'Unknown location'}")
        st.caption(f"Selected records last ingested: {utc_label(summary['last_ingested_at'])} · {freshness(summary['last_ingested_at'])}. "
                   f"Latest event age: {freshness(summary['latest_event'])}.")
        st.write(f"Last successful workflow (global): **{utc_label(coverage.get('last_successful_workflow'))}** · "
                 f"{freshness(coverage.get('last_successful_workflow'))}")
        left, right = st.columns(2)
        left.plotly_chart(charts.timeline(cached("timeline", filters, _data=data), "day"), width="stretch")
        right.plotly_chart(charts.distribution(cached("distribution", filters, _data=data), "magnitude"), width="stretch")
        st.caption("Only periods with stored events are plotted. Gaps do not establish zero earthquakes.")
        st.caption(f"Unknown magnitudes: {summary['unknown_magnitude']}; unknown depths: {summary['unknown_depth']}. "
                   "Averages exclude unknown values. Magnitude scales may differ between events.")
        st.subheader("Recent events")
        event_table(cached("events", filters, _data=data, limit=10))
    elif page == "Earthquake Map":
        rows = cached("events", filters, _data=data, limit=CHART_LIMIT)
        st.plotly_chart(charts.earthquake_map(rows), width="stretch")
        st.caption("Drag to pan; scroll to zoom. Locations are source epicenters, not damage zones. "
                   "Marker size is a display cue: negative magnitudes retain their values with a minimum visible size. "
                   "Gray markers have unknown magnitude. Basemap © OpenStreetMap contributors; tiles require internet.")
        if summary["total"] > CHART_LIMIT:
            st.warning(f"Showing the newest {CHART_LIMIT:,} matching events. Narrow filters for a complete map.")
    elif page == "Trends":
        interval = st.selectbox("Frequency interval", ("day", "week", "month"))
        st.plotly_chart(charts.timeline(cached("timeline", filters, _data=data, interval=interval), interval), width="stretch")
        st.caption("Only periods with stored events are plotted; missing periods do not prove zero earthquakes. Weeks start Monday (UTC).")
        left, right = st.columns(2)
        left.plotly_chart(charts.distribution(cached("distribution", filters, _data=data), "magnitude"), width="stretch")
        right.plotly_chart(charts.distribution(cached("distribution", filters, _data=data, measure="depth_km"), "depth_km"), width="stretch")
        st.plotly_chart(charts.scatter(cached("events", filters, _data=data, limit=CHART_LIMIT)), width="stretch")
        st.caption(f"Scatter uses up to the newest {CHART_LIMIT:,} events with both measurements present; "
                   "frequency and distributions aggregate all matches. Descriptive analysis only; no predictions.")
        st.subheader("Strongest events")
        event_table(cached("events", filters, _data=data, sort="Magnitude", limit=20))
    elif page == "Explorer":
        a, b = st.columns(2)
        sort = a.selectbox("Sort by", ("Newest", "Oldest", "Magnitude", "Depth"))
        page_size = b.selectbox("Rows per page", (25, 50, 100), index=1)
        total_pages = max(1, (summary["total"] + page_size - 1) // page_size)
        if total_pages > 100001:
            st.info("Navigation is capped at 100,001 pages. Narrow filters to reach the complete selection.")
        # No persistent page key: changing filters cannot leave an invalid old offset.
        page_number = st.number_input("Page", min_value=1, max_value=min(total_pages, 100001), value=1, step=1)
        event_table(cached("events", filters, _data=data, sort=sort, limit=page_size, offset=(page_number - 1) * page_size))
        st.caption(f"Page {page_number:,} of {total_pages:,}. CSV contains every matching event, not just this page.")
        if summary["total"] <= EXPORT_LIMIT:
            payload = cached("export", filters, _data=data, sort=sort)
            st.download_button("Download filtered CSV", payload, "quaketrack-events.csv", "text/csv")
        else:
            st.info(f"Narrow the filters to {EXPORT_LIMIT:,} events or fewer to export the complete selection.")


def main():
    st.set_page_config(page_title="QuakeTrack · Earthquake Intelligence", page_icon="🌐", layout="wide")
    st.title("QuakeTrack")
    st.markdown("#### Global Earthquake Intelligence")
    st.caption("Explore the record. Understand the pipeline. USGS observations, from source to insight.")
    with st.sidebar:
        st.markdown("## QuakeTrack")
        st.radio("Navigation", PAGES, key="page")
        if st.button("Refresh data"):
            cached.clear()
        st.caption("Read-only PostgreSQL · results cached for up to 60 seconds")
    try:
        with st.spinner("Reading earthquake data…"):
            render(get_data())
    except ValueError as error:
        # Only our validation messages are safe for display. Configuration errors are generic.
        if "POSTGRES" in str(error):
            st.error("Database configuration is incomplete. Check the local .env file; credentials are never displayed.")
        else:
            st.error(str(error))
    except SQLAlchemyError:
        st.error("Database unavailable or schema not ready. Check PostgreSQL on localhost:55432, "
                 "the local .env settings and reviewed migrations, then click Refresh data.")


if __name__ == "__main__":
    main()
