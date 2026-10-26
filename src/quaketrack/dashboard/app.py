"""Five read-only views with shared filters and compact, responsive presentation."""
from datetime import datetime, timezone
from html import escape

import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from .security import load_dashboard_settings
from . import charts, presentation as ui
from .data import CHART_LIMIT, EXPORT_LIMIT, DashboardData, Filters, dashboard_engine, freshness, utc_label

PAGES = ("Overview", "Earthquake Map", "Trends", "Explorer", "Pipeline Monitoring")
FILTER_KEYS = ("start", "end", "use_magnitude", "min_mag", "max_mag", "use_depth", "min_depth", "max_depth", "location", "unknown", "explorer_page", "map_event")


@st.cache_resource
def get_data():
    return DashboardData(dashboard_engine(load_dashboard_settings()))


@st.cache_data(ttl=60, max_entries=256, show_spinner=False)
def cached(method, filters=None, _data=None, **kwargs):
    return getattr(_data, method)(**kwargs) if filters is None else getattr(_data, method)(filters, **kwargs)


def number(value, suffix=""):
    return "Unknown" if value is None else f"{value:.2f}{suffix}"


def utc_rows(rows):
    return [{key: utc_label(value) if isinstance(value, datetime) else value for key, value in row.items()} for row in rows]


def reset_filters():
    for key in FILTER_KEYS:
        st.session_state.pop(key, None)
    for key, value in st.session_state.get("_filter_defaults", {}).items():
        st.session_state[key] = value


def open_explorer():
    st.session_state.page = "Explorer"


def initial_value(key, value):
    return None if key in st.session_state else value


def event_table(rows, *, technical=False):
    display = [{**row, "occurred_at": row["occurred_at"].strftime("%d %b %Y %H:%M:%S") if isinstance(row["occurred_at"], datetime) else row["occurred_at"]} for row in rows]
    columns = ("occurred_at", "magnitude", "depth_km", "place_description", "event_status", "source_url")
    if technical:
        columns += ("event_id", "magnitude_type", "latitude", "longitude")
    st.dataframe(display, column_order=columns, hide_index=True, width="stretch", height="content", column_config={
        "occurred_at": st.column_config.TextColumn("Time (UTC)", width="medium"),
        "magnitude": st.column_config.NumberColumn("Mag.", format="%.2f", width="small"),
        "depth_km": st.column_config.NumberColumn("Depth · km", format="%.1f", width="small"),
        "place_description": st.column_config.TextColumn("Source location", width="large"),
        "event_status": st.column_config.TextColumn("Status", width="small"),
        "source_url": st.column_config.LinkColumn("USGS", display_text="Open", width="small"),
        "event_id": "Event ID", "magnitude_type": "Magnitude type", "latitude": "Latitude (°)", "longitude": "Longitude (°)"})


def filters_ui(coverage):
    today = datetime.now(timezone.utc).date()
    first = coverage["first_event"].date() if coverage["first_event"] else today
    last = coverage["last_event"].date() if coverage["last_event"] else today
    st.session_state._filter_defaults = dict(start=first,end=last,use_magnitude=False,
        min_mag=float(coverage["min_magnitude"] or 0),max_mag=float(coverage["max_magnitude"] or 0),
        use_depth=False,min_depth=float(coverage["min_depth"] or 0),max_depth=float(coverage["max_depth"] or 0),
        location="",unknown=True,explorer_page=1)
    with st.sidebar:
        with st.expander("Date range · UTC", expanded=True):
            start = st.date_input("Start date (UTC)", initial_value("start",first), key="start", format="DD/MM/YYYY")
            end = st.date_input("End date (UTC)", initial_value("end",last), key="end", format="DD/MM/YYYY")
            st.caption("Both dates included · all times UTC")
        with st.expander("Magnitude & depth", expanded=False):
            use_mag = st.checkbox("Filter magnitude", key="use_magnitude")
            a, b = st.columns(2)
            min_mag = a.number_input("Minimum magnitude", value=initial_value("min_mag",float(coverage["min_magnitude"] or 0)), disabled=not use_mag, key="min_mag", format="%.2f")
            max_mag = b.number_input("Maximum magnitude", value=initial_value("max_mag",float(coverage["max_magnitude"] or 0)), disabled=not use_mag, key="max_mag", format="%.2f")
            use_depth = st.checkbox("Filter depth", key="use_depth")
            a, b = st.columns(2)
            min_depth = a.number_input("Minimum depth (km)", value=initial_value("min_depth",float(coverage["min_depth"] or 0)), disabled=not use_depth, key="min_depth", format="%.3f")
            max_depth = b.number_input("Maximum depth (km)", value=initial_value("max_depth",float(coverage["max_depth"] or 0)), disabled=not use_depth, key="max_depth", format="%.3f")
            unknown = st.checkbox("Include unknown magnitude / depth", value=False if "unknown" in st.session_state else True, key="unknown", help="Includes missing measurements even with active bounds. Uncheck to require both measurements. Valid negative values are always retained.")
        location = st.text_input("Location contains", max_chars=200, key="location", placeholder="Search source place labels…")
        st.divider()
        st.caption(f"DATASET · {coverage['total']:,} events · {coverage['days']:,} event dates")
        st.caption(f"{first:%d %b %Y} → {last:%d %b %Y}")
        st.caption("USGS observations · no country inference\nRead-only queries · 60-second cache")
    if start is None or end is None:
        raise ValueError("Choose both a start date and an end date")
    return Filters.dates(start, end, min_magnitude=min_mag if use_mag else None, max_magnitude=max_mag if use_mag else None,
        min_depth=min_depth if use_depth else None, max_depth=max_depth if use_depth else None, location=location, include_unknown=unknown)


def filter_summary(filters, total):
    def bounds(low, high):
        return "any" if low is None and high is None else f"{low if low is not None else 'any'}–{high if high is not None else 'any'}"
    from datetime import timedelta
    st.caption(f"{total:,} matching events · {filters.start:%d %b %Y} → {filters.end - timedelta(days=1):%d %b %Y} · UTC dates included")
    st.html(f'<div class="qt-filterbar">Magnitude {escape(bounds(filters.min_magnitude, filters.max_magnitude))} · Depth {escape(bounds(filters.min_depth, filters.max_depth))} km · Place {escape(filters.location or "any")} · Unknown measurements {"included" if filters.include_unknown else "excluded"}</div>')


def show_coverage(data, filters):
    windows = cached("coverage_windows", filters, _data=data)
    if len(windows) > 10000:
        st.warning("Coverage detail exceeds 10,000 windows; narrow the dates. No complete-range claim is made.")
        return
    days = ui.complete_days(windows, filters)
    label = "; ".join(f"{count:,} whole UTC days at source minimum magnitude {threshold}" for threshold,count in sorted(days.items()) if count)
    partial = ", ".join(threshold for threshold,count in sorted(days.items()) if not count)
    st.caption("Verified load coverage: " + (label or "no completed full-day windows in this selection") +
        (f"; partial-day samples at minimum magnitude {partial}" if partial else "") + ". Coverage precedes dashboard filters; missing periods are never filled with invented zeros.")


def overview(data, filters, summary, coverage):
    ui.kpis(summary)
    a, b = st.columns((1.7, 1))
    with a, st.container(border=True):
        rows = cached("timeline", filters, _data=data)
        flags = ui.period_coverage(rows,cached("coverage_windows",filters,_data=data),filters,"day")
        st.plotly_chart(charts.timeline(rows,"day",period_flags=flags),width="stretch")
        st.caption("Amber: partial or unverified period. No missing periods are filled with zeros.")
    with b, st.container(border=True):
        st.plotly_chart(charts.distribution(cached("distribution", filters, _data=data), "magnitude"), width="stretch")
    a, b = st.columns((1, 1.7))
    with a, st.container(border=True):
        st.plotly_chart(charts.density(cached("spatial_density", filters, _data=data)), width="stretch")
        st.caption("10° coordinate cells. Source epicenters only; these are not countries or damage zones.")
    with b, st.container(border=True):
        ui.section("Recent activity", "Newest 8 matching events")
        event_table(cached("events", filters, _data=data, limit=8))
        st.button("Open complete Explorer →", on_click=open_explorer, key="open_explorer")
    with st.expander("Freshness, source updates & missing measurements"):
        st.write(f"Stored occurrence bounds: **{utc_label(coverage['first_event'])}** → **{utc_label(coverage['last_event'])}**")
        st.write(f"Latest event: **{utc_label(summary['latest_event'])}** · {freshness(summary['latest_event'])}")
        st.write(f"Most recent source update: **{utc_label(summary['latest_source_update'])}**")
        st.write(f"Selected rows last ingested: **{utc_label(summary['last_ingested_at'])}** · {freshness(summary['last_ingested_at'])}")
        st.caption(f"Unknown magnitudes: {summary['unknown_magnitude']}; unknown depths: {summary['unknown_depth']}. Means exclude nulls; reported magnitude scales can differ.")


def map_page(data, filters, summary):
    rows = cached("events", filters, _data=data, limit=CHART_LIMIT)
    offline = st.toggle("Offline coordinate view", key="offline_map", help="Uses longitude and latitude axes with no external map tiles.")
    st.plotly_chart(charts.earthquake_map(rows, offline=offline), width="stretch", config={"scrollZoom": True})
    st.caption("Drag to pan; scroll to zoom. Marker size indicates magnitude with a minimum visible size for negative/unknown values; gray means unknown. Epicenters are not damage zones. OpenStreetMap tiles require internet; use the offline view if unavailable.")
    if summary["total"] > CHART_LIMIT:
        st.warning(f"Showing newest {CHART_LIMIT:,} matches. Narrow filters for every event on the map.")
    options = [row["event_id"] for row in rows]
    by_id = {row["event_id"]: row for row in rows}
    if st.session_state.get("map_event") not in options:
        st.session_state.pop("map_event", None)
    selected = st.selectbox("Inspect an event", options, key="map_event", format_func=lambda key: f"{key} · {by_id[key].get('place_description') or 'Unknown location'}")
    if selected:
        row = by_id[selected]
        with st.container(border=True):
            a, b, c = st.columns(3)
            a.metric("Magnitude", number(row.get("magnitude")))
            b.metric("Depth", number(row.get("depth_km"), " km"))
            c.metric("Coordinates", f"{row['latitude']:.3f}, {row['longitude']:.3f}")
            st.write(row.get("place_description") or "Unknown source location")
            st.caption(f"{utc_label(row['occurred_at'])} · {row['event_id']} · status {row.get('event_status') or 'unknown'}")
            if (row.get("source_url") or "").startswith("https://earthquake.usgs.gov/"):
                st.link_button("Open USGS event", row["source_url"])


def trends(data, filters):
    interval = st.selectbox("Frequency interval", ("day", "week", "month"))
    with st.container(border=True):
        rows = cached("timeline", filters, _data=data, interval=interval)
        flags = ui.period_coverage(rows,cached("coverage_windows",filters,_data=data),filters,interval)
        st.plotly_chart(charts.timeline(rows,interval,period_flags=flags),width="stretch")
    st.caption("Amber: partial calendar period or unverified load coverage at the source threshold. Only observed periods are plotted; gaps do not prove zero earthquakes. Weeks begin Monday (UTC).")
    a, b = st.columns(2)
    with a, st.container(border=True):
        st.plotly_chart(charts.distribution(cached("distribution", filters, _data=data), "magnitude"), width="stretch")
    with b, st.container(border=True):
        st.plotly_chart(charts.distribution(cached("distribution", filters, _data=data, measure="depth_km"), "depth_km"), width="stretch")
    with st.container(border=True):
        st.plotly_chart(charts.scatter(cached("events", filters, _data=data, limit=CHART_LIMIT)), width="stretch")
    st.caption(f"Scatter: newest {CHART_LIMIT:,} matches with both measurements. Histograms/frequency use all matches. Descriptive comparisons only.")
    ui.section("Strongest events", "Top 20 matching observations")
    event_table(cached("events", filters, _data=data, sort="Magnitude", limit=20))


def explorer(data, filters, summary):
    a, b, c = st.columns((1, 1, 1.4))
    sort = a.selectbox("Sort by", ("Newest", "Oldest", "Magnitude", "Depth"))
    size = b.selectbox("Rows per page", (25, 50, 100), index=1)
    technical = c.checkbox("Show technical columns", help="Event ID, magnitude type, latitude and longitude")
    total_pages = max(1, (summary["total"] + size - 1) // size)
    if total_pages > 100001:
        st.info("Table navigation is capped at 100,001 pages. Narrow the filters for all results.")
    maximum = min(total_pages, 100001)
    if st.session_state.get("explorer_page", 1) > maximum:
        st.session_state.explorer_page = 1
    page = st.number_input("Page", min_value=1, max_value=maximum, value="min", step=1, key="explorer_page")
    event_table(cached("events", filters, _data=data, sort=sort, limit=size, offset=(page - 1) * size), technical=technical)
    st.caption(f"Page {page:,} of {total_pages:,} · displayed times are UTC; CSV retains full timestamp precision.")
    if summary["total"] <= EXPORT_LIMIT:
        st.download_button("Download filtered CSV", cached("export", filters, _data=data, sort=sort), "quaketrack-events.csv", "text/csv")
        st.caption("Export includes every matching event and all technical columns, independently of the displayed page.")
    else:
        st.info(f"Narrow the selection to {EXPORT_LIMIT:,} events or fewer for a complete CSV export.")


def monitoring(data, coverage):
    st.info("These are application database ingestion records, not live Airflow scheduler status. The DAG was left paused during verification. Check Airflow for its current state.")
    st.link_button("Open Airflow · localhost:18080", "http://localhost:18080")
    st.caption("Global operational history is independent of earthquake filters. Workflow parents and persistence loads remain separate.")
    result = cached("monitoring", _data=data)
    workflows, loads = result["workflows"], result["loads"]
    runs = [*workflows[:1], *loads[:1]]
    if runs:
        latest = max(runs, key=lambda row: row["started_at"])
        duration = latest.get("duration_seconds")
        if duration is None and latest.get("finished_at"):
            duration = (latest["finished_at"] - latest["started_at"]).total_seconds()
        a, b, c = st.columns(3)
        a.metric("Latest ingestion status", latest["status"])
        b.metric("Duration (seconds)", number(duration))
        c.metric("Run type", "Workflow" if latest.get("execution_key") else "Database load")
        st.caption(f"Run {latest.get('execution_key') or latest.get('run_id')} · {utc_label(latest['started_at'])}")
        columns = st.columns(5)
        for column,label,key in zip(columns,("Extracted","Inserted","Updated","Unchanged","Quarantined"),("records_fetched","records_inserted","records_updated","records_unchanged","records_rejected")):
            column.metric(label, f"{latest[key]:,}")
    else:
        st.warning("No ingestion runs are recorded yet.")
    st.caption(f"Last successful workflow: {utc_label(result['last_success'])} · Last successful DB load: {utc_label(coverage.get('last_successful_load'))}")
    a, b = st.columns(2)
    with a, st.container(border=True):
        ui.section("Reusable Gold datasets")
        st.dataframe(result.get("gold", []), hide_index=True, width="stretch")
    with b, st.container(border=True):
        ui.section("Verification evidence", "Historical check, not a live scan")
        evidence = ui.saved_validation(coverage)
        if evidence:
            st.caption(f"Checked {evidence['checked_at']} · dataset summary {'matches' if evidence['matches_current_summary'] else 'has changed; revalidate'}")
            st.write(f"Raw archives: {evidence['raw_integrity']} · Contract: {evidence['contract_compliance']}% · Spark/SQL: {evidence['spark_parity']}")
        else:
            st.caption("No saved validation evidence is available. No integrity, contract or Spark status is inferred.")
    ui.section("Recent workflow history", "Up to 50 parent runs · no double-counted totals")
    st.dataframe(utc_rows([{k:v for k,v in row.items() if k != "error_message"} for row in workflows]), hide_index=True, width="stretch")
    with st.expander("Incremental coverage checkpoints"):
        st.dataframe(utc_rows(result["state"]), hide_index=True, width="stretch")
    ui.section("Persistence load history", "Up to 50 loads")
    st.dataframe(utc_rows(loads), hide_index=True, width="stretch")


def render(data):
    coverage = cached("coverage", _data=data)
    ui.header(coverage)
    filters = filters_ui(coverage)
    ui.coverage_note(coverage)
    page = st.session_state.page
    ui.section(page, "USGS catalog observations" if page != "Pipeline Monitoring" else "Source → storage → analytics")
    if page == "Pipeline Monitoring":
        monitoring(data, coverage)
        return
    summary = cached("overview", filters, _data=data)
    filter_summary(filters, summary["total"])
    show_coverage(data, filters)
    if not summary["total"]:
        st.info("No events match these filters. Widen the date or measurement range, or clear location search.")
        return
    if page == "Overview":
        overview(data, filters, summary, coverage)
    elif page == "Earthquake Map":
        map_page(data, filters, summary)
    elif page == "Trends":
        trends(data, filters)
    elif page == "Explorer":
        explorer(data, filters, summary)
    st.caption(f"Data queried {utc_label(coverage.get('queried_at'))} · cache expires after 60 seconds · Refresh data updates queries")


def main():
    st.set_page_config(page_title="QuakeTrack · Earthquake Intelligence", page_icon="🌐", layout="wide")
    st.html(ui.CSS)
    with st.sidebar:
        st.html('<div class="qt-brand">⌁ QuakeTrack<small>EARTHQUAKE INTELLIGENCE</small></div>')
        st.radio("Navigation", PAGES, key="page", label_visibility="collapsed")
        a, b = st.columns(2)
        a.button("Reset filters", key="reset_filters", on_click=reset_filters, width="stretch")
        b.button("Refresh data", key="refresh_data", on_click=cached.clear, width="stretch")
    try:
        with st.container(key="qt_shell"), st.spinner("Reading earthquake data…"):
            render(get_data())
    except ValueError as error:
        if "POSTGRES" in str(error):
            st.error("Database configuration is incomplete. Check the local .env file; credentials are never displayed.")
        else:
            st.error(str(error))
    except SQLAlchemyError:
        st.error("Database unavailable or schema not ready. Check PostgreSQL on localhost:55432, the local .env settings and reviewed migrations, then click Refresh data.")


if __name__ == "__main__":
    main()
