"""Coverage gaps, responsive presentation controls and lossless data behavior."""
from datetime import date, datetime, timedelta, timezone
import json
from unittest.mock import patch

from streamlit.testing.v1 import AppTest

from quaketrack.config import ROOT
from quaketrack.dashboard.app import cached
from quaketrack.dashboard.data import Filters, utc_label
from quaketrack.dashboard.presentation import complete_days, saved_validation, period_coverage
from quaketrack.dashboard import charts
from test_dashboard import sample_data, NOW


def test_coverage_requires_full_days_merges_overlaps_and_keeps_thresholds_separate():
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    def window(left, right, threshold="4"):
        return dict(window_start=start+timedelta(hours=left), window_end=start+timedelta(hours=right),
                    minimum_magnitude=threshold, event_type="earthquake")
    filters = Filters.dates(date(2020,1,1),date(2020,1,4))
    windows = [window(0,12),window(12,24),window(0,24),window(48,72),window(72,78,"5")]
    assert complete_days(windows, filters) == {"4":2,"5":0}
    assert complete_days([window(0,12,"4"),window(12,24,"5")], filters) == {"4":0,"5":0}
    assert complete_days([], filters) == {}
    assert complete_days(windows, Filters.dates(date(2020,1,2),date(2020,1,2))) == {}


def test_coverage_uses_utc_even_for_offset_timestamps():
    zone = timezone(timedelta(hours=5,minutes=30))
    window=dict(window_start=datetime(2020,1,1,5,30,tzinfo=zone),
                window_end=datetime(2020,1,2,5,30,tzinfo=zone),minimum_magnitude="4",event_type="earthquake")
    assert complete_days([window],Filters.dates(date(2020,1,1),date(2020,1,1))) == {"4":1}


def test_partial_periods_are_flagged_without_changing_event_counts():
    start=datetime(2020,1,1,tzinfo=timezone.utc)
    windows=[dict(window_start=start,window_end=start+timedelta(days=2),minimum_magnitude="4",event_type="earthquake")]
    rows=[dict(period=start,events=10),dict(period=start+timedelta(days=2),events=2)]
    filters=Filters.dates(date(2020,1,1),date(2020,1,3))
    flags=period_coverage(rows,windows,filters,"day")
    assert flags==[True,False]
    figure=charts.timeline(rows,"day",period_flags=flags)
    assert list(figure.data[0].y)==[10,2] and figure.data[0].marker.color[1]=='#f6b95e'
    assert period_coverage(rows[:1],windows,filters,"month")==[False]
    assert charts.timeline(rows[:1],"month",period_flags=[False]).layout.xaxis.dtick=="M1"
    assert period_coverage(rows,windows,Filters(filters.start,filters.end,min_magnitude=2),"day")==[False,False]


def test_offline_map_uses_actual_coordinates_without_external_tiles_and_gaps_stay_gaps():
    row=dict(event_id="test-only",occurred_at=NOW,magnitude=-1,depth_km=-2,latitude=20,longitude=-120,place_description=None)
    figure=charts.earthquake_map([row],offline=True)
    assert figure.data[0].type == "scattergl"
    assert list(figure.data[0].x)==[-120] and list(figure.data[0].y)==[20]
    assert figure.data[0].customdata[0][2]==-2 and figure.data[0].marker.size[0]>0
    rows=[dict(period=NOW,events=2),dict(period=NOW+timedelta(days=3),events=1)]
    timeline=charts.timeline(rows,"day")
    assert list(timeline.data[0].y)==[2,1] and len(timeline.data[0].x)==2


def test_validation_evidence_is_explicitly_historical_and_detects_changed_dataset(tmp_path):
    path=tmp_path/'summary.json'
    coverage=dict(total=40,last_ingested_at=NOW,latest_source_update=NOW)
    assert saved_validation(coverage,path) is None
    path.write_text(json.dumps(dict(dataset=dict(total=40,last_ingested_at=utc_label(NOW),
        latest_source_update=utc_label(NOW)),checked_at=utc_label(NOW))),encoding='utf8')
    assert saved_validation(coverage,path)['matches_current_summary'] is True
    assert saved_validation({**coverage,'total':41},path)['matches_current_summary'] is False
    path.write_text('broken',encoding='utf8')
    assert saved_validation(coverage,path) is None


def test_reset_restores_all_shared_filters(caplog):
    cached.clear()
    data=sample_data()
    with patch('quaketrack.dashboard.app.get_data',return_value=data):
        app=AppTest.from_file(str(ROOT/'dashboard/app.py'),default_timeout=20).run()
        app.text_input(key='location').set_value('Japan').run()
        app.checkbox(key='use_magnitude').check().run()
        app.number_input(key='min_mag').set_value(0).run()
        app.checkbox(key='use_depth').check().run()
        app.checkbox(key='unknown').uncheck().run()
        app.button(key='reset_filters').click().run()
        assert app.text_input(key='location').value==''
        assert not app.checkbox(key='use_magnitude').value and not app.checkbox(key='use_depth').value
        assert app.checkbox(key='unknown').value and app.date_input(key='start').value==NOW.date()
        assert not app.exception
        assert 'Session State API' not in caplog.text


def test_refresh_clears_cached_queries_and_open_explorer_navigation_works():
    cached.clear()
    data=sample_data()
    with patch('quaketrack.dashboard.app.get_data',return_value=data):
        app=AppTest.from_file(str(ROOT/'dashboard/app.py'),default_timeout=20).run()
        initial=data.coverage.call_count
        app.button(key='refresh_data').click().run()
        assert data.coverage.call_count==initial+1
        app.button(key='open_explorer').click().run()
        assert app.radio(key='page').value=='Explorer' and not app.exception


def test_explorer_pagination_and_filter_change_clamp_offset():
    cached.clear()
    data=sample_data(120)
    with patch('quaketrack.dashboard.app.get_data',return_value=data):
        app=AppTest.from_file(str(ROOT/'dashboard/app.py'),default_timeout=20).run()
        app.radio(key='page').set_value('Explorer').run()
        app.number_input(key='explorer_page').set_value(3).run()
        assert data.events.call_args.kwargs['offset']==100
        data.overview.return_value['total']=1
        app.text_input(key='location').set_value('Japan').run()
        assert app.number_input(key='explorer_page').value==1
        assert data.events.call_args.kwargs['offset']==0 and not app.exception


def test_cleared_date_is_a_validation_message_without_an_exception():
    cached.clear()
    with patch('quaketrack.dashboard.app.get_data',return_value=sample_data()):
        app=AppTest.from_file(str(ROOT/'dashboard/app.py'),default_timeout=20).run()
        app.date_input(key='start').set_value(None).run()
        assert not app.exception and 'Choose both' in app.error[0].value
