"""Plotly figures retain source measurements; marker size is only a display cue."""
import math
import plotly.graph_objects as go

TEAL = "#2dd4bf"


def style(figure, title, x=None, y=None):
    figure.update_layout(title=dict(text=title,font=dict(size=15)), template="plotly_dark", height=290,
                         paper_bgcolor="#111f32",plot_bgcolor="#111f32",
                         margin=dict(l=10, r=15, t=45, b=35),
                         xaxis_title=x, yaxis_title=y, font=dict(family="Arial", size=12,color="#c7d5e7"))
    figure.update_xaxes(gridcolor="#24354a",zerolinecolor="#34465d")
    figure.update_yaxes(gridcolor="#24354a",zerolinecolor="#34465d")
    return figure


def timeline(rows, interval, *, period_flags=None):
    width = {"day": 0.8, "week": 5.6, "month": 22.4}[interval] * 86_400_000
    colors = [TEAL if flag else "#f6b95e" for flag in period_flags] if period_flags is not None else TEAL
    figure = style(go.Figure(go.Bar(x=[r["period"] for r in rows], y=[r["events"] for r in rows],
                                 width=width, marker_color=colors)), f"{ {'day':'Daily','week':'Weekly','month':'Monthly'}[interval]} activity", "Period start (UTC)", "Ingested events")
    if interval == "month":
        figure.update_xaxes(dtick="M1",tickformat="%b %Y")
    return figure


def distribution(rows, measure):
    width = 0.5 if measure == "magnitude" else 25
    labels = ["Unknown" if r["bin_start"] is None else f"{r['bin_start']:g} to <{r['bin_start'] + r.get('bin_width', width):g}" for r in rows]
    figure = style(go.Figure(go.Bar(x=labels, y=[r["events"] for r in rows], marker_color=TEAL)),
                 "Magnitude distribution" if measure == "magnitude" else "Depth distribution",
                 "Magnitude (reported scales)" if measure == "magnitude" else "Depth (km)", "Ingested events")
    figure.update_xaxes(tickangle=-20)
    return figure


def usable_points(rows):
    return [r for r in rows if all(isinstance(r.get(k), (int, float)) and math.isfinite(r[k])
                                   for k in ("latitude", "longitude"))
            and -90 <= r["latitude"] <= 90 and -180 <= r["longitude"] <= 180]


def earthquake_map(rows, *, offline=False):
    figure = go.Figure()
    for known in (True, False):
        points = [r for r in usable_points(rows) if (r.get("magnitude") is not None) == known]
        if not points:
            continue
        custom = [[str(r["occurred_at"]), r.get("magnitude"), r.get("depth_km"),
                   r.get("place_description") or "Unknown location", r.get("event_id")] for r in points]
        marker = dict(size=[8 + 3 * max(0, r["magnitude"]) if known else 8 for r in points], opacity=0.8)
        if known:
            marker.update(color=[r["magnitude"] for r in points], colorscale=[[0,"#40c5b2"],[0.5,"#5ca9f4"],[1,"#f3b663"]],
                          showscale=True, colorbar=dict(title="Magnitude"))
        else:
            marker["color"] = "#64748b"
        locations = dict(x=[r["longitude"] for r in points],y=[r["latitude"] for r in points]) if offline else dict(lat=[r["latitude"] for r in points],lon=[r["longitude"] for r in points])
        trace_type = go.Scattergl if offline else go.Scattermap
        figure.add_trace(trace_type(**locations,
            mode="markers", marker=marker, customdata=custom, name="Known magnitude" if known else "Unknown magnitude",
            hovertemplate="%{customdata[3]}<br>%{customdata[0]} UTC<br>Magnitude: %{customdata[1]}"
                          "<br>Depth: %{customdata[2]} km<extra></extra>"))
    figure.update_layout(template="plotly_dark",paper_bgcolor="#111f32",plot_bgcolor="#111f32",
                         font=dict(color="#c7d5e7"),height=510,margin=dict(l=0,r=0,t=15,b=10),legend=dict(orientation="h"))
    if offline:
        figure.update_xaxes(title="Longitude (°)",range=[-180,180],gridcolor="#27364a")
        figure.update_yaxes(title="Latitude (°)",range=[-90,90],gridcolor="#27364a")
    else:
        figure.update_layout(map=dict(style="open-street-map", center=dict(lat=10, lon=0), zoom=0.8))
    return figure


def density(rows):
    labels = [f"[{r['latitude_band']:g}, {r['latitude_band']+10:g})° lat · [{r['longitude_band']:g}, {r['longitude_band']+10:g})° lon" for r in reversed(rows)]
    return style(go.Figure(go.Bar(x=[r["events"] for r in reversed(rows)],y=labels,orientation="h",marker_color="#68a9ed")),
                 "Most active coordinate cells", "Ingested events", None)


def scatter(rows):
    points = [r for r in rows if r.get("magnitude") is not None and r.get("depth_km") is not None]
    return style(go.Figure(go.Scattergl(x=[r["depth_km"] for r in points], y=[r["magnitude"] for r in points],
                                     mode="markers", marker=dict(color=TEAL, size=9),
                                     text=[r.get("place_description") or "Unknown location" for r in points])),
                 "Magnitude versus depth", "Depth (km)", "Magnitude (reported scales)")
