import sys

from grafanalib import formatunits as UNITS, _gen
from grafanalib.core import (
    Dashboard,
    Templating,
    Template,
    Annotations,
    RowPanel,
    Panel,
    HeatmapColor,
    Tooltip,
    GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
)

from dashboard_builder import (
    Layout,
    timeseries_panel,
    target,
    template,
    Expr,
    expr_aggr,
    expr_sum_rate,
    heatmap_panel,
    yaxis,
)


def heatmap_color_warm() -> HeatmapColor:
    return HeatmapColor()


def expr_histogram_full(
    quantile: float,
    metrics: str,
    label_selectors: list[str] = [],
    by_labels: list[str] = [],
) -> Expr:
    # sum(rate(metrics_bucket{label_selectors}[$__rate_interval])) by (le)
    assert not metrics.endswith(
        "_bucket"
    ), f"'{metrics}' should not specify '_bucket' suffix manually"

    by_labels = list(filter(lambda label: label != "le", by_labels))
    sum_rate_of_buckets = expr_sum_rate(
        metrics + "_bucket",
        label_selectors=label_selectors,
        by_labels=by_labels + ["le"],
    )

    # histogram_quantile({quantile}, {sum_rate_of_buckets})
    return expr_aggr(
        metric=f"{sum_rate_of_buckets}",
        aggr_op="histogram_quantile",
        aggr_param=f"{quantile}",
        label_selectors=[],
        by_labels=[],
    ).extra(
        # Do not attach default label selector again.
        default_label_selectors=[]
    )


def create_gauge_panel(
    expr, title, unit_format=UNITS.NUMBER_FORMAT, labels=[]
) -> Panel:
    if isinstance(expr, str):
        expr = Expr(metric=expr)

    return timeseries_panel(
        title=title,
        targets=[target(expr, legend_format="{{instance}}")],
        unit=unit_format,
    )


def create_counter_panel(
    expr, title, unit_format=UNITS.NUMBER_FORMAT, labels=[]
) -> Panel:
    return timeseries_panel(
        title=title,
        targets=[target(expr_sum_rate(expr), legend_format="{{instance}}")],
        unit=unit_format,
    )


def create_heatmap_panel(
    metric_name,
    title,
    unit_format=yaxis(UNITS.SECONDS),
) -> Panel:
    return heatmap_panel(
        title,
        f"{metric_name}_bucket",
        yaxis=unit_format,
        color=heatmap_color_warm(),
        tooltip=Tooltip(),
    )


def create_row(name, metrics) -> RowPanel:
    layout = Layout(name)
    for i in range(0, len(metrics), 2):
        chunk = metrics[i : i + 2]
        layout.row(chunk)
    return layout.row_panel


def net_conn_manager() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_net_conn_out_time", "Time taken to establish an outgoing connection"
        ),
        create_heatmap_panel(
            "tycho_net_conn_in_time", "Time taken to establish an incoming connection"
        ),
        create_counter_panel(
            "tycho_net_conn_out_total",
            "Number of established outgoing connections over time",
        ),
        create_counter_panel(
            "tycho_net_conn_in_total",
            "Number of established incoming connections over time",
        ),
        create_counter_panel(
            "tycho_net_conn_out_fail_total",
            "Number of failed outgoing connections over time",
        ),
        create_counter_panel(
            "tycho_net_conn_in_fail_total",
            "Number of failed incoming connections over time",
        ),
        create_gauge_panel(
            "tycho_net_conn_active", "Number of currently active connections"
        ),
        create_gauge_panel(
            "tycho_net_conn_pending", "Number of currently pending connections"
        ),
        create_gauge_panel(
            "tycho_net_conn_partial", "Number of currently half-resolved connections"
        ),
        create_gauge_panel(
            "tycho_net_conn_pending_dials",
            "Number of currently pending connectivity checks",
        ),
        create_gauge_panel(
            "tycho_net_active_peers", "Number of currently active peers"
        ),
        create_gauge_panel("tycho_net_known_peers", "Number of currently known peers"),
    ]
    return create_row("Network Connection Manager", metrics)


def net_request_handler() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_net_in_queries_time", "Duration of incoming queries handlers"
        ),
        create_heatmap_panel(
            "tycho_net_in_messages_time", "Duration of incoming messages handlers"
        ),
        create_counter_panel(
            "tycho_net_in_queries_total", "Number of incoming queries over time"
        ),
        create_counter_panel(
            "tycho_net_in_messages_total", "Number of incoming messages over time"
        ),
        create_counter_panel(
            "tycho_net_in_datagrams_total", "Number of incoming datagrams over time"
        ),
        create_gauge_panel(
            "tycho_net_req_handlers", "Current number of incoming request handlers"
        ),
    ]
    return create_row("Network Request Handler", metrics)


def net_peer() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_net_out_queries_time", "Duration of outgoing queries"
        ),
        create_heatmap_panel(
            "tycho_net_out_messages_time", "Duration of outgoing messages"
        ),
        create_counter_panel(
            "tycho_net_out_queries_total", "Number of outgoing queries over time"
        ),
        create_counter_panel(
            "tycho_net_out_messages_total", "Number of outgoing messages over time"
        ),
        create_gauge_panel(
            "tycho_net_out_queries", "Current number of outgoing queries"
        ),
        create_gauge_panel(
            "tycho_net_out_messages", "Current number of outgoing messages"
        ),
    ]
    return create_row("Network Peers", metrics)


def net_dht() -> RowPanel:
    metrics = [
        create_counter_panel(
            "tycho_net_dht_in_req_total", "Number of incoming DHT requests over time"
        ),
        create_counter_panel(
            "tycho_net_dht_in_req_fail_total",
            "Number of failed incoming DHT requests over time",
        ),
        create_counter_panel(
            "tycho_net_dht_in_req_with_peer_info_total",
            "Number of incoming DHT requests with peer info over time",
        ),
        create_counter_panel(
            "tycho_net_dht_in_req_find_node_total",
            "Number of incoming DHT FindNode requests over time",
        ),
        create_counter_panel(
            "tycho_net_dht_in_req_find_value_total",
            "Number of incoming DHT FindValue requests over time",
        ),
        create_counter_panel(
            "tycho_net_dht_in_req_get_node_info_total",
            "Number of incoming DHT GetNodeInfo requests over time",
        ),
        create_counter_panel(
            "tycho_net_dht_in_req_store_value_total",
            "Number of incoming DHT Store requests over time",
        ),
    ]
    return create_row("Network DHT", metrics)


def core_block_strider() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_process_mc_block_time", "Masterchain block processing time"
        ),
        create_heatmap_panel(
            "tycho_process_shard_block_time",
            "Shard block processing time",
        ),
        create_heatmap_panel(
            "tycho_fetch_shard_block_time", "Shard block downloading time"
        ),
        create_heatmap_panel(
            "tycho_download_shard_blocks_time",
            "Total time to download all shard blocks",
        ),
        create_heatmap_panel(
            "tycho_process_shard_blocks_time", "Total time to process all shard blocks"
        ),
    ]
    return create_row("Core Block Strider", metrics)


def jrpc() -> RowPanel:
    metrics = [
        create_counter_panel(
            "tycho_rpc_in_req_total", "Number of incoming JRPC requests over time"
        ),
        create_counter_panel(
            "tycho_rpc_in_req_fail_total",
            "Number of failed incoming JRPC requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_get_capabilities_total",
            "Number of incoming JRPC getCapabilities requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_get_latest_key_block_total",
            "Number of incoming JRPC getLatestKeyBlock requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_get_blockchain_config_total",
            "Number of incoming JRPC getBlockchainConfig requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_get_status_total",
            "Number of incoming JRPC getStatus requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_get_timings_total",
            "Number of incoming JRPC getTimings requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_send_message_total",
            "Number of incoming JRPC sendMessage requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_get_contract_state_total",
            "Number of incoming JRPC getContractState requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_get_accounts_by_code_hash_total",
            "Number of incoming JRPC getAccountsByCodeHash requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_get_transactions_list_total",
            "Number of incoming JRPC getTransactionsList requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_get_transaction_total",
            "Number of incoming JRPC getTransaction requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_get_dst_transaction_total",
            "Number of incoming JRPC getDstTransaction requests over time",
        ),
    ]
    return create_row("JRPC", metrics)


def collator_do_collate() -> RowPanel:
    metrics = [
        create_counter_panel(
            "tycho_do_collate_msgs_exec_count_all_total",
            "Number of all executed messages over time",
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_exec_count_ext_total",
            "Number of executed external messages over time",
        ),
        create_counter_panel(
            "tycho_do_collate_tx_total",
            "Number of transactions over time",
        ),
        create_gauge_panel(
            "tycho_do_collate_int_msgs_queue_length", "Internal messages queue len"
        ),
        create_heatmap_panel("tycho_do_collate_total_time", "Total collation time"),
        create_heatmap_panel("tycho_do_collate_prepare_time", "Collation prepare time"),
        create_heatmap_panel(
            "tycho_do_collate_init_iterator_time", "Init iterator time"
        ),
        create_heatmap_panel("tycho_do_collate_execute_time", "Execution time"),
        create_heatmap_panel("tycho_do_collate_build_block_time", "Build block time"),
        create_heatmap_panel("tycho_do_collate_update_state_time", "Update state time"),
    ]
    return create_row("Collator Do Collate", metrics)


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
                query="label_values(tycho_net_known_peers, instance)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
        ]
    )


dashboard = Dashboard(
    "Tycho Node Metrics",
    templating=templates(),
    refresh="5s",
    panels=[
        collator_do_collate(),
        net_conn_manager(),
        net_request_handler(),
        net_peer(),
        net_dht(),
        core_block_strider(),
        jrpc(),
    ],
    annotations=Annotations(),
    uid="cdlaji62a1b0gb",
    version=9,
    schemaVersion=14,
    graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    timezone="browser",
).auto_panel_ids()

# open file as stream
if len(sys.argv) > 1:
    stream = open(sys.argv[1], "w")
else:
    stream = sys.stdout
# write dashboard to file
_gen.write_dashboard(dashboard, stream)
