#!/usr/bin/env python3
import sys

from dashboard_builder import Layout, timeseries_panel, target, template, Expr, DATASOURCE
from grafanalib import formatunits as UNITS, _gen
from grafanalib.core import (
    Dashboard,
    Templating,
    Template,
    Annotations,
    RowPanel,
    Panel,
    GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
)


def templates() -> Templating:
    return Templating(
        list=[
            Template(
                name="source",
                query="prometheus",
                type="datasource",
            ),
            template(
                name="instance",
                query="label_values(tycho_storage_faster_size_bytes, instance)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
        ]
    )


def faster_panel(metric: str, title: str, unit=UNITS.NUMBER_FORMAT) -> Panel:
    expr = Expr(metric)
    return timeseries_panel(
        title=title,
        targets=[target(expr, legend_format="{{instance}}")],
        unit=unit,
    )


def faster_metrics() -> RowPanel:
    layout = Layout("faster", repeat=None, collapsed=True)
    metrics = [
        faster_panel(
            "tycho_storage_faster_size_bytes", "FASTER size", UNITS.BYTES_IEC
        ),
        faster_panel(
            "tycho_storage_faster_num_active_sessions", "FASTER active sessions"
        ),
        faster_panel(
            "tycho_storage_faster_auto_compaction_scheduled",
            "FASTER auto compaction scheduled",
            UNITS.NONE_FORMAT,
        ),
        faster_panel(
            "tycho_storage_faster_hlog_max_size_reached",
            "FASTER hlog max size reached",
            UNITS.NONE_FORMAT,
        ),
        faster_panel(
            "tycho_storage_faster_hlog_begin_address",
            "FASTER hlog begin address",
            UNITS.BYTES_IEC,
        ),
        faster_panel(
            "tycho_storage_faster_hlog_tail_address",
            "FASTER hlog tail address",
            UNITS.BYTES_IEC,
        ),
        faster_panel(
            "tycho_storage_faster_hlog_head_address",
            "FASTER hlog head address",
            UNITS.BYTES_IEC,
        ),
        faster_panel(
            "tycho_storage_faster_hlog_safe_head_address",
            "FASTER hlog safe head address",
            UNITS.BYTES_IEC,
        ),
        faster_panel(
            "tycho_storage_faster_hlog_read_only_address",
            "FASTER hlog read-only address",
            UNITS.BYTES_IEC,
        ),
        faster_panel(
            "tycho_storage_faster_hlog_safe_read_only_address",
            "FASTER hlog safe read-only address",
            UNITS.BYTES_IEC,
        ),
        faster_panel(
            "tycho_storage_faster_hlog_flushed_until_address",
            "FASTER hlog flushed until address",
            UNITS.BYTES_IEC,
        ),
    ]
    for i in range(0, len(metrics), 2):
        layout.row(metrics[i : i + 2])
    return layout.row_panel


dashboard = Dashboard(
    "Tycho FASTER",
    templating=templates(),
    refresh="30s",
    panels=[faster_metrics()],
    annotations=Annotations(),
    uid="64853dc3ef534081",
    version=1,
    schemaVersion=14,
    graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    timezone="browser",
).auto_panel_ids()


if len(sys.argv) > 1:
    stream = open(sys.argv[1], "w")
else:
    stream = sys.stdout

_gen.write_dashboard(dashboard, stream)
