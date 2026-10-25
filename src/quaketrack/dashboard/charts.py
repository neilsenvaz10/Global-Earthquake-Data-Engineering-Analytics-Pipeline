"""Plotly figures retain source measurements; marker size is only a display cue."""
import math
import plotly.graph_objects as go

TEAL = "#087f8c"


def style(figure, title, x=None, y=None):
    figure.update_layout(title=title, template="plotly_white", height=380,
                         margin=dict(l=15, r=15, t=55, b=35),
                         xaxis_title=x, yaxis_title=y, font=dict(family="Arial", size=13))
    return figure


def timeline(rows, interval):
    width = {"day": 0.8, "week": 5.6, "month": 22.4}[interval] * 86_400_000
    return style(go.Figure(go.Bar(x=[r["period"] for r in rows], y=[r["events"] for r in rows],
                                 width=width, marker_color=TEAL)), f"{interval.title()} activity", "Period start (UTC)", "Ingested events")


def distribution(rows, measure):
    width = 0.5 if measure == "magnitude" else 25
    labels = ["Unknown" if r["bin_start"] is None else f"{r['bin_start']:g} to <{r['bin_start'] + r.get('bin_width', width):g}" for r in rows]
    return style(go.Figure(go.Bar(x=labels, y=[r["events"] for r in rows], marker_color=TEAL)),
                 "Magnitude distribution" if measure == "magnitude" else "Depth distribution",
                 "Magnitude (reported scales)" if measure == "magnitude" else "Depth (km)", "Ingested events")


def usable_points(rows):
    return [r for r in rows if all(isinstance(r.get(k), (int, float)) and math.isfinite(r[k])
                                   for k in ("latitude", "longitude"))
            and -90 <= r["latitude"] <= 90 and -180 <= r["longitude"] <= 180]


def earthquake_map(rows):
    figure = go.Figure()
    for known in (True, False):
        points = [r for r in usable_points(rows) if (r.get("magnitude") is not None) == known]
        if not points:
            continue
        custom = [[str(r["occurred_at"]), r.get("magnitude"), r.get("depth_km"),
                   r.get("place_description") or "Unknown location"] for r in points]
        marker = dict(size=[8 + 3 * max(0, r["magnitude"]) if known else 8 for r in points], opacity=0.8)
        if known:
            marker.update(color=[r["magnitude"] for r in points], colorscale="Viridis",
                          showscale=True, colorbar=dict(title="Magnitude"))
        else:
            marker["color"] = "#64748b"
        figure.add_trace(go.Scattermap(lat=[r["latitude"] for r in points], lon=[r["longitude"] for r in points],
            mode="markers", marker=marker, customdata=custom, name="Known magnitude" if known else "Unknown magnitude",
            hovertemplate="%{customdata[3]}<br>%{customdata[0]} UTC<br>Magnitude: %{customdata[1]}"
                          "<br>Depth: %{customdata[2]} km<extra></extra>"))
    figure.update_layout(map=dict(style="open-street-map", center=dict(lat=10, lon=0), zoom=0.8),
                         height=600, margin=dict(l=0, r=0, t=0, b=0), legend=dict(orientation="h"))
    return figure


def scatter(rows):
    points = [r for r in rows if r.get("magnitude") is not None and r.get("depth_km") is not None]
    return style(go.Figure(go.Scatter(x=[r["depth_km"] for r in points], y=[r["magnitude"] for r in points],
                                     mode="markers", marker=dict(color=TEAL, size=9),
                                     text=[r.get("place_description") or "Unknown location" for r in points])),
                 "Magnitude versus depth", "Depth (km)", "Magnitude (reported scales)")
