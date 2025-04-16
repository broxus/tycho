import sys
from typing import Union, List, Literal

from dashboard_builder import (
    Layout,
    timeseries_panel,
    target,
    template,
    Expr,
    Stat,
    expr_sum_rate,
    expr_sum_increase,
    expr_aggr_func,
    expr_avg,
    heatmap_panel,
    yaxis,
    expr_operator,
    expr_max,
    DATASOURCE,
)
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
    Target,
)


# todo: do something with this metrics
# tycho_core_last_mc_block_applied
# tycho_core_last_sc_block_applied
# tycho_core_last_sc_block_seqno
# tycho_core_last_sc_block_utime


def heatmap_color_warm() -> HeatmapColor:
    return HeatmapColor()


def generate_legend_format(labels: List[str]) -> str:
    """
    Generate a legend format string based on the provided labels.

    Args:
    labels (List[str]): A list of label strings.

    Returns:
    str: A legend format string including instance and provided labels with keys.
    """
    legend_format = "{{instance}}"
    for label in labels:
        key = label.split("=")[0]  # Extract the key part before '='
        legend_format += f" {key}:{{{{{key}}}}}"
    return legend_format


def create_gauge_panel(
    expr: Union[str, List[Union[str, Expr]], Expr],
    title: str,
    unit_format=UNITS.NUMBER_FORMAT,
    labels=[],
    legend_format: str | None = None,
) -> Panel:
    if isinstance(expr, str):
        expr = [Expr(metric=expr, label_selectors=labels)]
    elif isinstance(expr, list):
        expr = [
            Expr(metric=e, label_selectors=labels) if isinstance(e, str) else e
            for e in expr
        ]
    elif isinstance(expr, Expr):
        expr = [expr]
    else:
        raise TypeError(
            "expr must be a string, a list of strings, or a list of Expr objects."
        )

    if legend_format is None:
        legend_format = generate_legend_format(labels)

    targets = [target(e, legend_format=legend_format) for e in expr]

    return timeseries_panel(
        title=title,
        targets=targets,
        unit=unit_format,
    )


def create_counter_panel(
    expr: Union[str | Expr, List[Union[str, Expr]]],
    title: str,
    unit_format: str = UNITS.NUMBER_FORMAT,
    labels_selectors: List[str] = [],
    legend_format: str | None = None,
    by_labels: list[str] = ["instance"],
) -> Panel:
    """
    Create a counter panel for visualization.

    Args:
        expr (Union[str, List[Union[str, Expr]]]): Expression or list of expressions to visualize.
        title (str): Title of the panel.
        unit_format (str, optional): Format for the unit display. Defaults to UNITS.NUMBER_FORMAT.
        labels_selectors (List[str], optional): List of label selectors. Defaults to an empty list.
        legend_format (str | None, optional): Format for the legend. If None, it's generated automatically. Defaults to None.
        by_labels (list[str], optional): Labels to group by. Defaults to ["instance"].

    Returns:
        Panel: A timeseries panel object.
    """
    if legend_format is None:
        legend_format = generate_legend_format(labels_selectors)

    if isinstance(expr, str):
        targets = [
            target(
                expr_sum_rate(
                    expr, label_selectors=labels_selectors, by_labels=by_labels
                ),
                legend_format=legend_format,
            )
        ]
    elif isinstance(expr, list):
        if all(isinstance(e, str) for e in expr):
            targets = [
                target(
                    expr_sum_rate(
                        e, label_selectors=labels_selectors, by_labels=by_labels
                    ),
                    legend_format=legend_format,
                )
                for e in expr
            ]
        elif all(isinstance(e, Expr) for e in expr):
            targets = [target(e, legend_format=legend_format) for e in expr]
        else:
            raise ValueError("List elements must be all strings or all Expr objects.")
    elif isinstance(expr, Expr):
        targets = [target(expr, legend_format=legend_format)]
    else:
        raise TypeError(
            "expr must be a string, a list of strings, or a list of Expr objects."
        )

    return timeseries_panel(
        title=title,
        targets=targets,
        unit=unit_format,
    )


def create_percent_panel(
    metric1: str,
    metric2: str,
    title: str,
    group_by_labels: List[str] = ["instance"],
    label_selectors: List[str] = [],
    unit_format: str = UNITS.PERCENT_FORMAT,
) -> Panel:
    """
    create a panel showing the percentage of metric1 to metric2, grouped by specified labels.

    Args:
        metric1 (str): The first metric (numerator).
        metric2 (str): The second metric (denominator).
        title (str): Title of the panel.
        group_by_labels (List[str]): Labels to group by and match on.
        label_selectors (List[str]): Additional label selectors for both metrics.
        unit_format (str, optional): Format for the unit display. defaults to UNITS.PERCENT_FORMAT.

    Returns:
        Panel: A timeseries panel object showing the percentage.
    """
    expr1 = expr_sum_rate(
        metric1, label_selectors=label_selectors, by_labels=group_by_labels
    )
    expr2 = expr_sum_rate(
        metric2, label_selectors=label_selectors, by_labels=group_by_labels
    )

    percent_expr = expr_operator(expr_operator(expr1, "/", expr2), "*", "100")

    legend_format = "{{" + "}} - {{".join(group_by_labels) + "}}"

    percent_target = target(percent_expr, legend_format=legend_format)

    return timeseries_panel(title=title, targets=[percent_target], unit=unit_format)


def create_heatmap_panel(
    metric_name,
    title,
    unit_format=yaxis(UNITS.SECONDS),
    labels=[],
) -> Panel:
    return heatmap_panel(
        title,
        f"{metric_name}_bucket",
        yaxis=unit_format,
        color=heatmap_color_warm(),
        tooltip=Tooltip(),
        label_selectors=labels,
        rate_interval="10s",  # todo: update this if scrape interval changes
    )


# Type alias for accepted quantiles
ACCEPTED_QUANTILES = {"0", "0.5", "0.9", "0.95", "0.99", "0.999", "1"}
AcceptedQuantile = Literal["0", "0.5", "0.9", "0.95", "0.99", "0.999", "1"]


def create_heatmap_quantile_panel(
    metric_name: str,
    title: str,
    unit_format: str = UNITS.NUMBER_FORMAT,
    quantile: AcceptedQuantile = "0.95",
) -> Panel:
    """
    Create a heatmap quantile panel for the given metric.

    Args:
        metric_name (str): Name of the metric to visualize.
        title (str): Title of the panel.
        unit_format (str, optional): Unit format for the panel. Defaults to UNITS.NUMBER_FORMAT.
        quantile (AcceptedQuantile, optional): Quantile to use (as an integer 0-100). Defaults to 95.

    Returns:
        Panel: A configured grafanalib Panel object.

    Raises:
        ValueError: If the quantile is not one of the accepted values.
    """

    if quantile not in ACCEPTED_QUANTILES:
        raise ValueError(f"Quantile must be one of {ACCEPTED_QUANTILES}")

    legend_format = f"{{{{instance}}}} p{quantile}"
    quantile_expr = f'quantile="{quantile}"'

    return timeseries_panel(
        title=title,
        targets=[
            target(
                expr=Expr(metric_name, label_selectors=[quantile_expr]),
                legend_format=legend_format,
            )
        ],
        unit=unit_format,
    )


def create_row(
    name: str, metrics, repeat: str | None = None, collapsed=True
) -> RowPanel:
    layout = Layout(name, repeat=repeat, collapsed=collapsed)
    for i in range(0, len(metrics), 2):
        chunk = metrics[i : i + 2]
        layout.row(chunk)
    return layout.row_panel


def blockchain_stats() -> RowPanel:
    def expr_aggr_avg_rate(metric: str) -> Expr:
        rate = expr_sum_rate(metric)
        return expr_aggr_func(f"{rate}", "avg", "avg_over_time", by_labels=[]).extra(
            default_label_selectors=[]
        )

    first_row = [
        timeseries_panel(
            targets=[
                target(expr_aggr_avg_rate("tycho_bc_txs_total"), legend_format="avg")
            ],
            title="Transactions Rate",
            unit="tx/s",
            legend_display_mode="hidden",
        ),
        timeseries_panel(
            targets=[
                target(
                    expr_aggr_avg_rate("tycho_bc_ext_msgs_total"),
                    legend_format="received",
                ),
                target(
                    expr_aggr_avg_rate("tycho_do_collate_msgs_error_count_ext"),
                    legend_format="failed",
                ),
                target(
                    expr_aggr_avg_rate("tycho_do_collate_msgs_skipped_count_ext"),
                    legend_format="skipped",
                ),
                target(
                    expr_aggr_avg_rate("tycho_do_collate_ext_msgs_expired_count"),
                    legend_format="expired",
                ),
            ],
            title="External Messages Rate",
            unit="msg/s",
            legend_display_mode="hidden",
        ),
        Stat(
            targets=[
                Target(
                    expr=f"""{
                        expr_max(
                            "tycho_last_applied_block_seqno",
                            label_selectors=['workchain="-1"'],
                            by_labels=[],
                        )
                    }""",
                    legendFormat="Last Applied MC Block",
                    instant=True,
                    datasource=DATASOURCE,
                ),
                Target(
                    expr=f"""{
                        expr_max(
                            "tycho_last_processed_to_anchor_id",
                            label_selectors=['workchain="-1"'],
                            by_labels=[],
                        )
                    }""",
                    legendFormat="Last Used Anchor",
                    instant=True,
                    datasource=DATASOURCE,
                ),
            ],
            graphMode="area",
            textMode="value_and_name",
            reduceCalc="lastNotNull",
            format=UNITS.NONE_FORMAT,
        ),
    ]

    second_row = [
        timeseries_panel(
            targets=[
                target(
                    expr_avg(
                        "tycho_storage_store_block_data_size",
                        label_selectors=['quantile="0.5"'],
                        by_labels=[],
                    ),
                    legend_format="P50",
                ),
                target(
                    expr_avg(
                        "tycho_storage_store_block_data_size",
                        label_selectors=['quantile="0.999"'],
                        by_labels=[],
                    ),
                    legend_format="P99",
                ),
            ],
            title="Block Data Size",
            unit=UNITS.BYTES,
            legend_display_mode="hidden",
        ),
        timeseries_panel(
            targets=[
                target(
                    expr_aggr_func(
                        "tycho_do_collate_blocks_count",
                        "avg",
                        "rate",
                        label_selectors=['workchain=~"$workchain"'],
                        by_labels=["workchain"],
                    ),
                    legend_format="{{workchain}}",
                )
            ],
            title="Blocks Rate",
            unit="blocks/s",
            legend_display_mode="hidden",
        ),
        timeseries_panel(
            targets=[
                target(
                    expr_aggr_func(
                        "tycho_mempool_engine_current_round",
                        "avg",
                        "rate",
                        by_labels=[],
                    ),
                    legend_format="rate",
                )
            ],
            title="Mempool Rounds Rate",
            unit="rounds/s",
            legend_display_mode="hidden",
        ),
    ]

    layout = Layout("Stats", repeat=None, collapsed=True)
    layout.row(first_row)
    layout.row(second_row)
    return layout.row_panel


def core_bc() -> RowPanel:
    metrics = [
        timeseries_panel(
            targets=[
                target(
                    expr_operator(
                        'timestamp(up{instance=~"$instance"})',
                        "-",
                        Expr("tycho_core_last_mc_block_utime"),
                    ),
                    legend_format="{{instance}}",
                )
            ],
            unit=UNITS.SECONDS,
            title="Mc block processing lag",
        ),
        create_gauge_panel(
            "tycho_last_applied_block_seqno",
            "Last applied block seqno",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_last_processed_to_anchor_id",
            "Last processed to anchor",
            labels=['workchain=~"$workchain"'],
        ),
        create_counter_panel("tycho_bc_txs_total", "Number of transactions over time"),
        create_counter_panel(
            "tycho_bc_ext_msgs_total", "Number of external messages over time"
        ),
        create_counter_panel("tycho_bc_msgs_total", "Number of all messages over time"),
        create_counter_panel(
            "tycho_bc_contract_deploy_total", "Number of contract deployments over time"
        ),
        create_counter_panel(
            "tycho_bc_contract_delete_total", "Number of contract deletions over time"
        ),
        create_heatmap_quantile_panel(
            "tycho_bc_total_gas_used", "Total gas used per block", quantile="1"
        ),
        create_heatmap_quantile_panel(
            "tycho_bc_in_msg_count",
            "Number of inbound messages per block",
            quantile="0.999",
        ),
        create_heatmap_quantile_panel(
            "tycho_bc_out_msg_count",
            "Number of outbound messages per block",
            quantile="0.999",
        ),
        create_heatmap_quantile_panel(
            "tycho_bc_out_in_msg_ratio",
            "Out/In message ratio per block",
            quantile="0.999",
        ),
        create_heatmap_quantile_panel(
            "tycho_bc_out_msg_acc_ratio",
            "Out message/Account ratio per block",
            quantile="0.999",
        ),
        # todo: pie chart?
        create_heatmap_quantile_panel(
            "tycho_bc_software_version",
            "Software version per block",
            quantile="0.999",
        ),
    ]
    return create_row("Blockchain", metrics)


def net_traffic() -> RowPanel:
    legend_format = "{{instance}} - {{service}}"
    by_labels = ["service", "instance"]
    metrics = [
        create_counter_panel(
            "tycho_private_overlay_tx",
            "Private overlay traffic sent",
            UNITS.BYTES_SEC_IEC,
            legend_format=legend_format,
            by_labels=by_labels,
        ),
        create_counter_panel(
            "tycho_private_overlay_rx",
            "Private overlay traffic received",
            UNITS.BYTES_SEC_IEC,
            legend_format=legend_format,
            by_labels=by_labels,
        ),
        create_counter_panel(
            "tycho_public_overlay_tx",
            "Public overlay traffic sent",
            UNITS.BYTES_SEC_IEC,
            legend_format=legend_format,
            by_labels=by_labels,
        ),
        create_counter_panel(
            "tycho_public_overlay_rx",
            "Public overlay traffic received",
            UNITS.BYTES_SEC_IEC,
            legend_format=legend_format,
            by_labels=by_labels,
        ),
        create_counter_panel(
            "tycho_rpc_broadcast_external_message_tx_bytes_total",
            "RPC broadcast external message traffic sent",
            UNITS.BYTES_SEC_IEC,
        ),
        create_counter_panel(
            "tycho_rpc_broadcast_external_message_rx_bytes_total",
            "RPC broadcast external message traffic received",
            UNITS.BYTES_SEC_IEC,
        ),
    ]
    return create_row("network: Traffic", metrics)


def core_blockchain_rpc() -> RowPanel:
    methods = [
        "getNextKeyBlockIds",
        "getBlockFull",
        "getBlockDataChunk",
        "getNextBlockFull",
        "getKeyBlockProof",
        "getArchiveInfo",
        "getArchiveChunk",
        "getPersistentStateInfo",
        "getPersistentStatePart",
    ]
    metrics = [
        create_gauge_panel(
            "tycho_core_overlay_client_validators_to_resolve",
            "Number of validators to resolve",
        ),
        create_gauge_panel(
            "tycho_core_overlay_client_resolved_validators",
            "Number of resolved validators",
        ),
        create_gauge_panel(
            "tycho_core_overlay_client_target_validators",
            "Number of selected broadcast targets",
        ),
        create_heatmap_panel(
            "tycho_core_overlay_client_validator_ping_time", "Time to ping validator"
        ),
        create_gauge_panel(
            expr=[
                "tycho_broadcast_timeout",
            ],
            title="Broadcast Timeout",
            unit_format=UNITS.SECONDS,
            legend_format="{{instance}} - {{kind}}",
        ),
    ]
    metrics += [
        create_heatmap_panel(
            "tycho_blockchain_rpc_method_time",
            f"Blockchain RPC {method} time",
            labels=[f'method="{method}"'],
        )
        for method in methods
    ]
    return create_row("blockchain: RPC", metrics)


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
    return create_row("network: Connection Manager", metrics)


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
            "tycho_net_in_requests_rejected_total",
            "Number of rejected incoming messages over time",
        ),
        create_gauge_panel(
            "tycho_net_req_handlers", "Current number of incoming request handlers"
        ),
        create_gauge_panel(
            "tycho_net_req_handlers_per_peer",
            "Current number of incoming request handlers per peer",
        ),
    ]
    return create_row("network: Request Handler", metrics)


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
    return create_row("network: Peers", metrics)


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
    return create_row("network: DHT", metrics)


def core_block_strider() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_core_process_strider_step_time",
            "Time to process block strider step",
        ),
        create_heatmap_panel(
            "tycho_core_provider_cleanup_time",
            "Time to cleanup block providers",
        ),
        create_heatmap_panel(
            "tycho_core_download_mc_block_time", "Masterchain block downloading time"
        ),
        create_heatmap_panel(
            "tycho_core_prepare_mc_block_time", "Masterchain block preparing time"
        ),
        create_heatmap_panel(
            "tycho_core_process_mc_block_time", "Masterchain block processing time"
        ),
        create_heatmap_panel(
            "tycho_core_download_sc_block_time", "Shard block downloading time"
        ),
        create_heatmap_panel(
            "tycho_core_prepare_sc_block_time",
            "Shard block preparing time",
        ),
        create_heatmap_panel(
            "tycho_core_process_sc_block_time",
            "Shard block processing time",
        ),
        create_heatmap_panel(
            "tycho_core_download_sc_blocks_time",
            "Total time to download all shard blocks",
        ),
        create_heatmap_panel(
            "tycho_core_process_sc_blocks_time",
            "Total time to process all shard blocks",
        ),
        create_heatmap_panel(
            "tycho_core_state_applier_prepare_block_time",
            "Time to prepare block by ShardStateApplier",
        ),
        create_heatmap_panel(
            "tycho_core_state_applier_handle_block_time",
            "Time to handle block by ShardStateApplier",
        ),
        create_heatmap_panel(
            "tycho_core_archive_handler_prepare_block_time",
            "Time to prepare block by ArchiveHandler",
        ),
        create_heatmap_panel(
            "tycho_core_archive_handler_handle_block_time",
            "Time to handle block by ArchiveHandler",
        ),
        create_heatmap_panel(
            "tycho_core_subscriber_handle_state_time",
            "Total time to handle state by all subscribers",
        ),
        create_heatmap_panel(
            "tycho_core_subscriber_handle_archive_time",
            "Total time to handle archive by all subscribers",
        ),
        create_heatmap_panel(
            "tycho_core_apply_block_time",
            "Time to apply and save block state",
        ),
        create_heatmap_panel(
            "tycho_core_metrics_subscriber_handle_block_time",
            "Time to handle block by MetricsSubscriber",
        ),
        create_heatmap_panel(
            "tycho_core_check_block_proof_time", "Check block proof time"
        ),
    ]
    return create_row("block strider: Core Metrics", metrics)


def storage() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_storage_load_cell_time", "Time to load cell from storage"
        ),
        create_counter_panel(
            expr_sum_rate("tycho_storage_load_cell_time_count"),
            "Number of load_cell calls",
            UNITS.OPS_PER_SEC,
        ),
        create_heatmap_panel(
            "tycho_storage_get_cell_from_rocksdb_time", "Time to load cell from RocksDB"
        ),
        create_counter_panel(
            expr_sum_rate("tycho_storage_get_cell_from_rocksdb_time_count"),
            "Number of cache missed cell loads",
            UNITS.OPS_PER_SEC,
        ),
        timeseries_panel(
            title="Storage Cache Hit Rate",
            targets=[
                target(
                    expr=expr_operator(
                        expr_operator(
                            "1",
                            "-",
                            expr_operator(
                                expr_sum_rate(
                                    "tycho_storage_get_cell_from_rocksdb_time_count",
                                ),
                                "/",
                                expr_sum_rate(
                                    "tycho_storage_load_cell_time_count",
                                ),
                            ),
                        ),
                        "*",
                        "100",
                    ),
                    legend_format="Hit Rate",
                )
            ],
            unit=UNITS.PERCENT_FORMAT,
        ),
        create_counter_panel(
            "tycho_storage_raw_cells_cache_size",
            "Raw cells cache size",
            UNITS.BYTES_IEC,
        ),
        create_heatmap_quantile_panel(
            "tycho_storage_store_block_data_size",
            "Block data size",
            UNITS.BYTES_IEC,
            "0.999",
        ),
        create_heatmap_quantile_panel(
            "tycho_storage_cell_count",
            "Number of new cells from merkle update",
            quantile="0.999",
        ),
        create_heatmap_panel(
            "tycho_storage_state_update_time", "Time to write state update to rocksdb"
        ),
        create_heatmap_panel(
            "tycho_storage_state_store_time",
            "Time to store single root with rocksdb write etc",
        ),
        create_heatmap_panel(
            "tycho_storage_cell_in_mem_store_time", "Time to store cell without write"
        ),
        create_heatmap_panel(
            "tycho_storage_batch_write_time", "Time to write merge in write batch"
        ),
        create_heatmap_quantile_panel(
            "tycho_storage_state_update_size_bytes",
            "State update size",
            UNITS.BYTES,
            "0.999",
        ),
        create_heatmap_quantile_panel(
            "tycho_storage_state_update_size_predicted_bytes",
            "Predicted state update size",
            UNITS.BYTES,
            "0.999",
        ),
        create_heatmap_panel(
            "tycho_storage_state_store_time", "Time to store state with cell traversal"
        ),
        create_heatmap_panel("tycho_gc_states_time", "Time to garbage collect state"),
        timeseries_panel(
            targets=[
                target(
                    expr_operator(
                        Expr(
                            metric="tycho_core_last_mc_block_seqno",
                        ),
                        "- on(instance, job)",
                        Expr("tycho_gc_states_seqno"),
                    ),
                    legend_format="{{instance}}",
                )
            ],
            unit="States",
            title="States GC lag",
        ),
        create_gauge_panel(
            "tycho_core_mc_blocks_gc_lag", "Blocks GC lag", unit_format="Blocks"
        ),
        create_gauge_panel("tycho_core_blocks_gc_tail_len", "GC diffs tail len"),
        create_heatmap_panel(
            "tycho_storage_move_into_archive_time", "Time to move into archive"
        ),
        create_heatmap_panel(
            "tycho_storage_commit_archive_time", "Time to commit archive"
        ),
        create_heatmap_panel(
            "tycho_storage_split_block_data_time", "Time to split block data"
        ),
        create_gauge_panel(
            "tycho_storage_cells_tree_cache_size", "Cells tree cache size"
        ),
        create_counter_panel(
            "tycho_compaction_keeps", "Number of not deleted cells during compaction"
        ),
        create_counter_panel(
            "tycho_compaction_removes", "Number of deleted cells during compaction"
        ),
        create_counter_panel(
            "tycho_storage_state_gc_count", "number of deleted states during gc"
        ),
        create_counter_panel(
            "tycho_storage_state_gc_cells_count", "number of deleted cells during gc"
        ),
        create_heatmap_panel(
            "tycho_storage_state_gc_time", "time spent to gc single root"
        ),
        create_heatmap_panel(
            "tycho_storage_load_block_data_time", "Time to load block data"
        ),
        create_counter_panel(
            "tycho_storage_load_block_data_time_count",
            "Number of load_block_data calls",
        ),
        create_percent_panel(
            "tycho_storage_block_cache_hit_total",
            "tycho_storage_load_block_total",
            "Block cache hit ratio",
        ),
    ]
    return create_row("Storage", metrics)


def jrpc() -> RowPanel:
    methods = [
        "GetCapabilities",
        "GetLatestKeyBlock",
        "GetBlockchainConfig",
        "GetStatus",
        "GetTimings",
        "SendMessage",
        "GetContractState",
        "GetAccountsByCodeHash",
        "GetTransactionsList",
        "GetTransaction",
        "GetDstTransaction",
    ]

    metrics = [
        create_heatmap_panel(
            "tycho_rpc_state_update_time", "Time to update RPC state on block"
        ),
        create_heatmap_panel(
            "tycho_rpc_state_update_accounts_cache_time",
            "Time to update RPC accounts cache on state",
        ),
        create_heatmap_panel(
            "tycho_storage_rpc_prepare_batch_time",
            "Time to prepare RPC storage update batch",
        ),
        create_heatmap_panel(
            "tycho_storage_rpc_execute_batch_time",
            "Time to execute RPC storage update batch",
        ),
        create_counter_panel(
            expr_sum_rate("tycho_jrpc_request_time_count"),
            "Number of incoming JRPC requests over time",
        ),
        create_counter_panel(
            "tycho_rpc_in_req_fail_total",
            "Number of failed incoming JRPC requests over time",
        ),
    ]
    for method in methods:
        metrics.append(
            create_counter_panel(
                expr="tycho_jrpc_request_time_count",
                title=f"JRPC {method} requests over time",
                labels_selectors=[f'method="{method}"'],
                legend_format="{{instance}}",
            )
        )

    return create_row("JRPC", metrics)


def jrpc_timings() -> RowPanel:
    return create_row(
        "JRPC: timings",
        [
            create_heatmap_panel(
                "tycho_jrpc_request_time",
                "JRPC $method time",
                labels=['method=~"$method"'],
            )
        ],
        repeat="method",
    )


def collator_finalize_block() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_collator_finalize_block_time",
            "Total time to finalize block",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_build_account_blocks_and_msgs_time",
            "Build in parallel account blocks, InMsgDescr, OutMsgDescr",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_build_account_blocks_time",
            "only Build account blocks",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_build_in_msgs_time",
            "only Build InMsgDescr",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_build_out_msgs_time",
            "only Build OutMsgDescr",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_finish_build_mc_state_extra_time",
            "Build McStateExtra",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_build_state_update_time",
            "Compute MerkleUpdate",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_create_merkle_update_time",
            "inc. Create MerkleUpdate",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_build_block_time",
            "Build Block",
            labels=['workchain=~"$workchain"'],
        ),
    ]
    return create_row("collator: Finalize Block", metrics)


def collator_params_metrics() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "tycho_do_collate_msgs_exec_params_buffer_limit",
            "Params: msgs buffer limit",
        ),
        create_gauge_panel(
            "tycho_do_collate_msgs_exec_params_group_limit", "Params: group limit"
        ),
        create_gauge_panel(
            "tycho_do_collate_msgs_exec_params_group_vert_size",
            "Params: group vertical size",
        ),
        create_gauge_panel(
            "tycho_do_collate_msgs_exec_params_group_slots_fractions",
            "Params: group slots fractions (by partitions)",
            labels=['par_id=~"$partition"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_msgs_exec_params_externals_expire_timeout",
            "Params: externals expire timeout",
        ),
        create_gauge_panel(
            "tycho_do_collate_msgs_exec_params_open_ranges_limit",
            "Params: open reader ranges limit",
        ),
        create_gauge_panel(
            "tycho_do_collate_msgs_exec_params_par_0_int_msgs_count_limit",
            "Params: partition 0 internal messages count limit",
        ),
        create_gauge_panel(
            "tycho_do_collate_msgs_exec_params_par_0_ext_msgs_count_limit",
            "Params: partition 0 external messages count limit",
        ),
        create_gauge_panel(
            "tycho_do_collate_int_buffer_limits_max_count",
            "Params: internals buffers max messages count",
            labels=['par_id=~"$partition"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_ext_buffer_limits_max_count",
            "Params: externals buffers max messages count",
            labels=['par_id=~"$partition"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_int_buffer_limits_slots_count",
            "Params: internals buffers slots count",
            labels=['par_id=~"$partition"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_ext_buffer_limits_slots_count",
            "Params: externals buffers slots count",
            labels=['par_id=~"$partition"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_int_buffer_limits_slot_vert_size",
            "Params: internals buffers slots vertical size",
            labels=['par_id=~"$partition"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_ext_buffer_limits_slot_vert_size",
            "Params: externals buffers slots vertical size",
            labels=['par_id=~"$partition"'],
        ),
    ]
    return create_row("collator: Parameters", metrics)


def collation_metrics() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "tycho_node_in_current_vset",
            "Node is in current validator set",
        ),
        create_gauge_panel(
            "tycho_last_block_seqno",
            "Last received/synced_to/collated block seqno",
            labels=['workchain=~"$workchain"'],
            legend_format="{{instance}} workchain: {{workchain}} src: {{src}}",
        ),
        create_counter_panel(
            "tycho_collator_sync_to_applied_mc_block_count",
            "Number of syncs to applied mc block",
        ),
        create_counter_panel(
            "tycho_collator_block_mismatch_count",
            "Number of mismatched blocks",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_collator_sync_is_running",
            "Collator: sync is running",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_collator_refill_messages_is_running",
            "Collator: refill messages is running",
            labels=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_tx_total",
            "Number of transactions over time",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_blocks_with_limits_reached_count",
            "Number of blocks with limits reached",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_tx_per_block",
            "Number of transactions per block",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_accounts_per_block",
            "Number of accounts per block",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_collator_shard_blocks_count_btw_anchors",
            "Number of Shard Blocks before import next anchor",
        ),
        create_gauge_panel(
            "tycho_collator_import_next_anchor_count",
            "Number of imported anchors per tick",
        ),
        create_counter_panel(
            "tycho_collator_anchor_import_cancelled_count",
            "Number of anchor import cancelled",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_collator_anchor_import_skipped_count",
            "Number of anchor import skipped",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_block_diff_tail_len",
            "Diff tail length",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_blocks_count_in_collation_manager_cache",
            "Blocks count in collation manager cache",
        ),
    ]
    return create_row("collator: Collation Metrics", metrics)


def collator_execution_metrics() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "tycho_do_collate_msgs_exec_buffer_messages_count",
            "Messages count in exec buffer",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_exec_msgs_groups_per_block",
            "Number of msgs groups per block",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_one_tick_group_messages_count",
            "One exec tick group messages count",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_one_tick_group_horizontal_size",
            "One exec tick group horizontal size",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_one_tick_group_mean_vert_size",
            "One exec tick MEAN group vertical size",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_one_tick_group_max_vert_size",
            "One exec tick MAX group vertical size",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_one_tick_account_msgs_exec_mean_time",
            "MEAN exec time in group",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_one_tick_account_msgs_exec_max_time",
            "MAX exec time in group",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_processed_upto_ext_ranges",
            "Externals ProcessedUpto ranges count (by partitions)",
            labels=['workchain=~"$workchain"', 'par_id=~"$partition"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_processed_upto_int_ranges",
            "Internals ProcessedUpto ranges count (by partitions)",
            labels=['workchain=~"$workchain"', 'par_id=~"$partition"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_msgs_exec_buffer_messages_count_by_partitions",
            "Messages count in exec buffer (by partitions)",
            labels=['workchain=~"$workchain"', 'par_id=~"$partition"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_accounts_count_in_partitions",
            "Accounts count in isolated partitions",
            labels=['workchain=~"$workchain"', 'par_id=~"$partition"'],
        ),
    ]
    return create_row("collator: Execution Metrics", metrics)


def collator_message_metrics() -> RowPanel:
    metrics = [
        create_counter_panel(
            "tycho_do_collate_msgs_exec_count_all",
            "All executed msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_collator_ext_msgs_imported_queue_size",
            "Ext msgs imported queue size",
            labels=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_collator_ext_msgs_imported_count",
            "Imported Ext msgs count from mempool",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_ext_msgs_expired_count",
            "Ext msgs expired count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_read_count_ext",
            "Read Ext msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_read_count_ext_by_partitions",
            "Read Ext msgs count (by partitions)",
            labels_selectors=['workchain=~"$workchain"', 'par_id=~"$partition"'],
            by_labels=["instance", "par_id"],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_exec_count_ext",
            "Executed Ext msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_error_count_ext",
            "Ext msgs error count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_skipped_count_ext",
            "Ext msgs skipped count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_placeholder",
            "placeholder",
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_read_count_int",
            "Read Int msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_read_count_int_by_partitions",
            "Read Int msgs count (by partitions)",
            labels_selectors=['workchain=~"$workchain"', 'par_id=~"$partition"'],
            by_labels=["instance", "par_id"],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_exec_count_int",
            "Executed Int msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_placeholder",
            "placeholder",
        ),
        create_counter_panel(
            "tycho_do_collate_new_msgs_created_count",
            "Created NewInt msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_new_msgs_inserted_count",
            "Inserted NewInt msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_read_count_new_int",
            "Read NewInt msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_read_count_new_int_by_partitions",
            "Read NewInt msgs count (by partitions)",
            labels_selectors=['workchain=~"$workchain"', 'par_id=~"$partition"'],
            by_labels=["instance", "par_id"],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_exec_count_new_int",
            "Executed NewInt msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
    ]
    return create_row("collator: Message Metrics", metrics)


def collator_queue_metrics() -> RowPanel:
    legend_format = "{{instance}} wc: {{workchain}}"
    legend_format_partition = "{{instance}} par_id: {{partition}}"
    metrics = [
        create_gauge_panel(
            "tycho_do_collate_int_msgs_queue_calc", "Calculated Internal queue len"
        ),
        create_counter_panel(
            "tycho_do_collate_int_enqueue_count", "Enqueued int msgs count"
        ),
        create_counter_panel(
            "tycho_do_collate_int_dequeue_count", "Dequeued int msgs count"
        ),
        create_gauge_panel(
            "tycho_internal_queue_processed_upto",
            "Queue clean until",
            legend_format=legend_format,
        ),

        create_heatmap_panel(
            "tycho_internal_queue_gc_execute_task_time", "GC execute time"
        ),
        create_gauge_panel(
            "tycho_internal_queue_gc_state_size",
            "Total GC state size",
        ),
        create_heatmap_panel(
            "tycho_internal_queue_statistics_load_time",
            "Committed statistics load time",
        ),
        create_heatmap_panel(
            "tycho_internal_queue_separated_statistics_load_time",
            "Separated statistics load time",
        ),
        create_counter_panel(
            "tycho_collator_queue_adapter_iterators_count", "Iterators count"
        ),
        create_heatmap_panel(
            "tycho_internal_queue_create_iterator_time",
            "Create queue iterator: total time",
        ),
        create_heatmap_panel(
            "tycho_internal_queue_snapshot_time",
            "Create queue iterator: incl. snapshot time",
        ),
        create_heatmap_panel(
            "tycho_internal_queue_commited_state_iterator_create_time",
            "Create queue iterator: incl. committed state iterator create time",
        ),
        create_counter_panel(
            "tycho_internal_queue_apply_diff_add_statistics_accounts_count",
            "Apply diff: add statistics accounts count",
            legend_format=legend_format_partition,
            by_labels=["instance", "partition"],
        ),
        create_heatmap_panel(
            "tycho_internal_queue_apply_diff_add_statistics_time", 
            "Apply diff: add statistics time",
        ),
        create_heatmap_panel(
            "tycho_internal_queue_apply_diff_add_messages_time",
            "Apply diff: add messages time",
        ),
        create_heatmap_panel(
            "tycho_internal_queue_write_diff_time", "Apply diff: write diff time"
        ),
    ]
    return create_row("collator: Queue Metrics", metrics)


def collator_time_metrics() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "tycho_do_collate_block_time_diff",
            "Block time diff",
            UNITS.SECONDS,
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_ext_msgs_time_diff",
            "Externals time diff",
            UNITS.SECONDS,
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_from_prev_block_time",
            "Time elapsed from prev block",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_from_prev_anchor_time",
            "Time elapsed from prev anchor",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_overhead_time",
            "Collation flow overhead",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_prepare_working_state_update_time",
            "Prepare WorkingState update",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_resume_collation_time",
            "Resume collation",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_build_new_state_time",
            "Build Pure State for next collation",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_wait_for_working_state_time",
            "Wait for updated WorkingState",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_try_collate_next_master_block_time",
            "Try collate next master block",
        ),
        create_heatmap_panel(
            "tycho_collator_try_collate_next_shard_block_time",
            "Try collate next shard block",
        ),
        create_heatmap_panel(
            "tycho_collator_import_next_anchor_time",
            "Import next anchor time",
            labels=['workchain=~"$workchain"'],
        ),
    ]
    return create_row("collator: Time diffs", metrics)


def collator_wu_metrics() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "tycho_do_collate_wu_on_prepare",
            "Wu spent on prepare",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_price_on_prepare",
            "Wu price on prepare",
            labels=['workchain=~"$workchain"'],
            unit_format=UNITS.NANO_SECONDS,
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_on_prepare_read_ext_msgs",
            "Wu spent on read externals",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_price_on_prepare_read_ext_msgs",
            "Wu price on read externals",
            labels=['workchain=~"$workchain"'],
            unit_format=UNITS.NANO_SECONDS,
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_on_prepare_read_existing_int_msgs",
            "Wu spent on read existing internals",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_price_on_prepare_read_existing_int_msgs",
            "Wu price on read existing internals",
            labels=['workchain=~"$workchain"'],
            unit_format=UNITS.NANO_SECONDS,
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_on_prepare_read_new_int_msgs",
            "Wu spent on read new internals",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_price_on_prepare_read_new_int_msgs",
            "Wu price on read new internals",
            labels=['workchain=~"$workchain"'],
            unit_format=UNITS.NANO_SECONDS,
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_on_prepare_add_msgs_to_groups",
            "Wu spent on add msgs to groups",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_price_on_prepare_add_msgs_to_groups",
            "Wu price on add msgs to groups",
            labels=['workchain=~"$workchain"'],
            unit_format=UNITS.NANO_SECONDS,
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_on_execute",
            "Wu spent on execute",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_price_on_execute",
            "Wu price on execute total",
            labels=['workchain=~"$workchain"'],
            unit_format=UNITS.NANO_SECONDS,
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_price_on_execute_txs",
            "Wu price on execute in vm",
            labels=['workchain=~"$workchain"'],
            unit_format=UNITS.NANO_SECONDS,
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_price_on_process_txs",
            "Wu price on process executed txs",
            labels=['workchain=~"$workchain"'],
            unit_format=UNITS.NANO_SECONDS,
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_on_finalize",
            "Wu spent on finalize",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_price_on_finalize",
            "Wu price on finalize",
            labels=['workchain=~"$workchain"'],
            unit_format=UNITS.NANO_SECONDS,
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_total",
            "Wu spent total on prepare, execute and finalize",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_wu_price_total",
            "Wu price total",
            labels=['workchain=~"$workchain"'],
            unit_format=UNITS.NANO_SECONDS,
        ),
    ]
    return create_row("collator: Work units calculation", metrics)


def collator_core_operations_metrics() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_do_collate_total_time",
            "Total collation time",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_prepare_time",
            "Collation prepare time",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_execute_time",
            "Execution time",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_fill_msgs_total_time",
            "Execution time: incl Fill messages",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_init_iterator_time",
            "Execution time: incl Fill messages: init iterator",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_read_int_msgs_time",
            "Execution time: incl Fill messages: read existing",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_read_ext_msgs_time",
            "Execution time: incl Fill messages: read externals",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_read_new_msgs_time",
            "Execution time: incl Fill messages: read new",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_add_to_msg_groups_time",
            "Execution time: incl Fill messages: add to msg groups",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_exec_msgs_total_time",
            "Execution time: incl Execute messages",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_process_txs_total_time",
            "Execution time: incl Process transactions",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_create_queue_diff_time",
            "async Create message queue diff",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_apply_queue_diff_time",
            "async Apply message queue diff",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_build_statistics_time",
            "async Apply message queue diff: inc. Build statistics",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_block_time",
            "Finalize block",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_handle_block_candidate_time",
            "Handle block candidate",
            labels=['workchain=~"$workchain"'],
        ),
    ]
    return create_row("collator: Core Operations Metrics", metrics)


def collator_misc_operations_metrics() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_collator_handle_collated_block_candidate_time",
            "Handle collated block candidate",
        ),
        create_heatmap_panel(
            "tycho_collator_handle_block_from_bc_time",
            "Handle block from bc",
        ),
        create_heatmap_panel(
            "tycho_collator_sync_to_applied_mc_block_time",
            "Sync to applied master block",
        ),
        create_heatmap_panel(
            "tycho_collator_refresh_collation_sessions_time",
            "Refresh collation sessions",
        ),
        create_heatmap_panel(
            "detect_next_collation_step_time",
            "Detect next collation step",
        ),
        create_heatmap_panel(
            "tycho_collator_enqueue_mc_block_collation_time",
            "Enqueue master block collation",
        ),
        create_heatmap_panel(
            "tycho_collator_handle_validated_master_block_time",
            "Handle validated master block",
        ),
        create_heatmap_panel(
            "tycho_collator_commit_queue_diffs_time",
            "Commit queue diffs",
        ),
    ]
    return create_row("collator: Misc Operations Metrics", metrics)


def collator_special_transactions_metrics() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_do_collate_execute_tick_time",
            "Execute Tick special transactions",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_execute_tock_time",
            "Execute Tock special transactions",
            labels=['workchain=~"$workchain"'],
        ),
    ]
    return create_row("collator: Special Transactions Metrics", metrics)


def collator_commit_block_metrics() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_collator_commit_valid_master_block_time",
            "Commit valid master block",
        ),
        create_heatmap_panel(
            "tycho_collator_extract_master_block_subgraph_time",
            "Extract master block subgraph",
        ),
        create_heatmap_panel(
            "tycho_collator_send_blocks_to_sync_time", "Send blocks to sync"
        ),
        create_heatmap_panel(
            "tycho_collator_send_blocks_to_sync_commit_diffs_time",
            "Commit queue diffs after send",
        ),
    ]
    return create_row("collator: Commit block Metrics", metrics)


def collator_state_adapter_metrics() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_collator_state_adapter_prepare_block_proof_time",
            "Prepare block proof",
        ),
        create_heatmap_panel(
            "tycho_collator_state_adapter_save_block_proof_time", "Save block proof"
        ),
        create_heatmap_panel(
            "tycho_collator_state_store_state_root_time", "Store state root"
        ),
        create_heatmap_panel("tycho_collator_state_load_state_time", "Load state"),
        create_heatmap_panel("tycho_collator_state_load_block_time", "Load block"),
        create_heatmap_panel(
            "tycho_collator_state_load_queue_diff_time", "Load queue diff"
        ),
        create_heatmap_panel(
            "tycho_collator_state_adapter_handle_state_time", "Handle state update"
        ),
    ]
    return create_row("collator: State Adapter Metrics", metrics)


def validator() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_validator_validate_block_time", "Time to validate a block"
        ),
        create_heatmap_panel(
            "tycho_validator_exchange_signature_time",
            "Time of a single signature exchange",
        ),
        create_heatmap_panel(
            "tycho_validator_receive_signature_time",
            "Time to receive a valid signature from peer",
        ),
        create_counter_panel(
            "tycho_validator_block_exchanges_in_total",
            "Number of received block exchanges (full) over time",
        ),
        create_counter_panel(
            "tycho_validator_cache_exchanges_in_total",
            "Number of received cache exchanges (partial) over time",
        ),
        create_counter_panel(
            "tycho_validator_miss_exchanges_in_total",
            "Number of received exchanges out of known range over time",
        ),
        create_counter_panel(
            "tycho_validator_invalid_signatures_in_total",
            "Number of received invalid signatures over time",
        ),
        create_gauge_panel(
            "tycho_validator_sessions_active",
            "Number of currently active validator sessions",
        ),
        create_gauge_panel(
            "tycho_validator_block_slots", "Number of currently active block slots"
        ),
        create_gauge_panel(
            "tycho_validator_cache_slots", "Number of currently active cache slots"
        ),
        create_counter_panel(
            "tycho_validator_invalid_signatures_cached_total",
            "Number of cached invalid signatures",
        ),
    ]
    return create_row("Validator", metrics)


def mempool_rounds() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "tycho_mempool_engine_current_round",
            "Engine: current round (is always started at consensus round or next one)",
        ),
        create_gauge_panel(
            "tycho_mempool_last_anchor_round",
            "Adapter: last anchor round",
        ),
        create_gauge_panel(
            "tycho_mempool_consensus_current_round",
            "Consensus: round determined by Broadcast Filter",
        ),
        create_gauge_panel(
            "tycho_mempool_rounds_consensus_ahead_top_known",
            "Consensus ahead of top known block: pause clause",
        ),
        create_gauge_panel(
            "tycho_mempool_rounds_dag_length",
            "DAG length in memory",
        ),
        create_gauge_panel(
            "tycho_mempool_rounds_consensus_ahead_committed",
            "Consensus ahead of committed: commit latency",
        ),
        create_gauge_panel(
            "tycho_mempool_rounds_engine_ahead_last_trigger",
            "Engine ahead of last anchor trigger: leaders finish 3 rounds in a row",
        ),
        create_gauge_panel(
            "tycho_mempool_rounds_committed_ahead_top_known",
            "Committed ahead of top known block: confirm block duration",
        ),
        create_gauge_panel(
            "tycho_mempool_rounds_engine_ahead_proof_chain",
            "Engine ahead of last linked anchor proof: local gaps in leader chain",
        ),
        create_gauge_panel(
            "tycho_mempool_rounds_consensus_ahead_storage_round",
            "Consensus ahead of storage: history to keep",
        ),
        create_gauge_panel(
            "tycho_mempool_engine_run_count",
            "Engine: (re)start count at genesis round",
            legend_format="{{instance}} - {{genesis_round}}",
        ),
        create_gauge_panel(
            "tycho_mempool_rounds_db_cleaned",
            "DB: deleted rounds",
            legend_format="{{instance}} - {{kind}}",
        ),
    ]
    return create_row("Mempool rounds", metrics)


def mempool_payload_rates() -> RowPanel:
    metrics = [
        create_counter_panel(
            "tycho_mempool_msgs_unique_count",
            "Adapter: unique externals count",
        ),
        create_counter_panel(
            "tycho_mempool_msgs_unique_bytes",
            "Adapter: unique externals size",
            unit_format=UNITS.BYTES_IEC,
        ),
        create_counter_panel(
            "tycho_mempool_msgs_duplicates_count",
            "Adapter: removed duplicate externals count",
        ),
        create_counter_panel(
            "tycho_mempool_msgs_duplicates_bytes",
            "Adapter: removed duplicate externals size",
            unit_format=UNITS.BYTES_IEC,
        ),
        create_counter_panel(
            "tycho_mempool_point_payload_count",
            "Engine: points payload count",
        ),
        create_counter_panel(
            "tycho_mempool_point_payload_bytes",
            "Engine: points payload size",
            unit_format=UNITS.BYTES_IEC,
        ),
        create_counter_panel(
            "tycho_mempool_evicted_externals_count",
            "Input buffer: evicted externals count",
        ),
        create_counter_panel(
            "tycho_mempool_evicted_externals_size",
            "Input buffer: evicted externals size",
            unit_format=UNITS.BYTES_IEC,
        ),
        create_heatmap_panel(
            "tycho_mempool_input_buffer_spent_time",
            "Input buffer: time msg spent in queue",
        ),
        create_heatmap_panel(
            "tycho_mempool_adapter_parse_anchor_history_time",
            "Adapter: parse anchor history into cells",
        ),
    ]
    return create_row("Mempool payload rates", metrics)


def mempool_engine_rates() -> RowPanel:
    metrics = [
        create_counter_panel(
            "tycho_mempool_points_produced",
            "Engine: produced points",
        ),
        create_counter_panel(
            "tycho_mempool_commit_anchors",
            "Engine: committed anchors",
        ),
        create_counter_panel(
            "tycho_mempool_signatures_collected_count",
            "Broadcaster: collected signatures in response",
        ),
        create_counter_panel(
            "tycho_mempool_collected_broadcasts_count",
            "Collector: timely received broadcasts",
        ),
        create_counter_panel(
            "tycho_mempool_broadcaster_retry_count",
            "Broadcaster: signature request retries",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_signatures_rejected_count",
                range_selector="$__interval",
            ),
            "Broadcaster: rejections received (total at moment)",
        ),
        create_counter_panel(
            "tycho_mempool_signing_current_round_count",
            "Signer: current round broadcasts signed",
        ),
        create_counter_panel(
            "tycho_mempool_collected_includes_count",
            "Round task: collected includes to produce point",
        ),
        create_counter_panel(
            "tycho_mempool_signing_prev_round_count",
            "Signer: previous round broadcasts signed",
        ),
        create_counter_panel(
            "tycho_mempool_signing_postponed",
            "Signer: postponed points - time or round are in future",
            legend_format="{{instance}} - {{kind}}",
        ),
        create_counter_panel(
            "tycho_mempool_signing_rejected",
            "Signer: rejected point - round too old or node not in v_set",
            legend_format="{{instance}} - {{kind}}",
        ),
        create_gauge_panel(
            "tycho_mempool_produced_point_time_skew",
            "Producer: point time ahead of clock",
            unit_format=UNITS.MILLI_SECONDS,
        ),
    ]
    return create_row("Mempool engine rates", metrics)


def mempool_engine() -> RowPanel:
    metrics = [
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_rounds_dag_behind_consensus",
                range_selector="$__interval",
            ),
            "Dag: rounds behind consensus",
        ),
        create_heatmap_panel(
            "tycho_mempool_engine_round_time",
            "Engine: round duration",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_points_no_proof_produced", range_selector="$__interval"
            ),
            "Engine: produced points without proof (total at moment)",
        ),
        create_heatmap_panel(
            "tycho_mempool_engine_produce_time",
            "Engine: produce point task duration",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_engine_produce_skipped",
                label_selectors=['kind=~"$kind"'],
                range_selector="$__interval",
                by_labels=["kind", "instance"],
            ),
            "Engine: points to produce skipped (total at moment)",
            legend_format="{{instance}} - {{kind}}",
        ),
        create_heatmap_panel(
            "tycho_mempool_engine_commit_time",
            "Engine: commit duration",
        ),
        create_gauge_panel(
            "tycho_mempool_commit_latency_rounds",
            "Engine committed anchor: rounds latency (max over batch)",
        ),
        create_heatmap_panel(
            "tycho_mempool_commit_anchor_latency_time",
            "Engine committed anchor: time latency",
        ),
        create_counter_panel(
            "tycho_mempool_points_verify_ok", "Verifier: verify() OK points (rate)"
        ),
        create_counter_panel(
            "tycho_mempool_points_resolved_ok",
            "Engine: first valid points resolved (rate)",
            labels_selectors=['ord="first"'],
            legend_format="{{instance}}",
        ),
        create_heatmap_panel(
            "tycho_mempool_verifier_verify_time",
            "Verifier: verify() point structure and author's sig",
        ),
        create_heatmap_panel(
            "tycho_mempool_verifier_validate_time",
            "Verifier: validate() point dependencies in DAG and all-1 sigs",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_points_verify_err",
                label_selectors=['kind=~"$kind"'],
                range_selector="$__interval",
                by_labels=["kind", "instance"],
            ),
            "Verifier: verify() errors (total at moment)",
            legend_format="{{instance}} - {{kind}}",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_points_resolved_err",
                label_selectors=['kind=~"$kind"', 'ord="first"'],
                range_selector="$__interval",
                by_labels=["kind", "instance"],
            ),
            "Engine: first resolved point errors (total at moment)",
            legend_format="{{instance}} - {{kind}}",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_points_resolved_ok",
                label_selectors=['ord="alt"'],
                range_selector="$__interval",
                by_labels=["instance"],
            ),
            "Engine: alt valid points resolved (total at moment)",
            legend_format="{{instance}}",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_points_resolved_err",
                label_selectors=['kind=~"$kind"', 'ord="alt"'],
                range_selector="$__interval",
                by_labels=["kind", "instance"],
            ),
            "Engine: alt resolved point errors (total at moment)",
            legend_format="{{instance}} - {{kind}}",
        ),
    ]
    return create_row("Mempool engine", metrics)


def mempool_intercom() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_mempool_broadcast_query_dispatcher_time",
            "Dispatcher: Broadcast send",
        ),
        create_heatmap_panel(
            "tycho_mempool_broadcast_query_responder_time",
            "Responder: Broadcast accept",
        ),
        # == Network tasks - multiple per round == #
        create_heatmap_panel(
            "tycho_mempool_signature_query_dispatcher_time",
            "Dispatcher: Signature request",
        ),
        create_heatmap_panel(
            "tycho_mempool_download_query_dispatcher_time",
            "Dispatcher: Download request",
        ),
        create_heatmap_panel(
            "tycho_mempool_signature_query_responder_data_time",
            "Responder: Signature send: send ready or sign or reject",
        ),
        create_heatmap_panel(
            "tycho_mempool_download_query_responder_some_time",
            "Responder: Download send: Some(point)",
        ),
        create_heatmap_panel(
            "tycho_mempool_signature_query_responder_pong_time",
            "Responder: Signature send: no point or try later",
        ),
        create_heatmap_panel(
            "tycho_mempool_download_query_responder_none_time",
            "Responder: Download send: None",
        ),
        # == Download tasks - multiple per round == #
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_download_task_count", range_selector="$__interval"
            ),
            "Downloader: tasks (unique point id) (total at moment)",
        ),
        create_heatmap_panel(
            "tycho_mempool_download_task_time", "Downloader: tasks duration"
        ),
        create_counter_panel(
            expr_aggr_func(
                metric="tycho_mempool_download_depth_rounds",
                aggr_op="max",
                func="increase",
                label_selectors=[],
                range_selector="$__interval",
                by_labels=["instance"],
            ),
            "Downloader: point depth (max rounds from current) (total at moment)",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_download_not_found_responses",
                range_selector="$__interval",
            ),
            "Downloader: received None in response (total at moment)",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_download_aborted_on_exit_count",
                range_selector="$__interval",
            ),
            "Downloader: queries aborted (on task completion) (total at moment)",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_download_query_failed_count",
                range_selector="$__interval",
            ),
            "Downloader: queries network error (total at moment)",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_download_unreliable_responses",
                range_selector="$__interval",
            ),
            "Downloader: unreliable responses (total at moment)",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_signatures_unreliable_count",
                range_selector="$__interval",
            ),
            "Broadcaster: unreliable signatures in response (total at moment)",
        ),
    ]
    return create_row("Mempool communication", metrics)


def mempool_peers() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "tycho_mempool_peers_resolved",
            "Peers: all resolved",
        ),
        create_gauge_panel(
            "tycho_mempool_peers_resolving",
            "Peers: resolving",
        ),
        create_gauge_panel(
            "tycho_mempool_bcast_receivers",
            "Peers: broadcast receivers",
        ),
        create_gauge_panel(
            "tycho_mempool_peer_in_next_vset",
            "Peer in next set",
            unit_format=UNITS.YES_NO,
        ),
        create_gauge_panel(
            "tycho_mempool_peer_vsubset_change",
            "Next subset start round (decrease to prev means unset)",
            labels=['epoch="next"'],
            legend_format="{{instance}}",
        ),
        create_gauge_panel(
            "tycho_mempool_peer_in_next_vsubset",
            "Next subset peer position (0 for not included)",
        ),
        create_gauge_panel(
            "tycho_mempool_peer_vsubset_change",
            "Current subset start round",
            labels=['epoch="curr"'],
            legend_format="{{instance}}",
        ),
        create_gauge_panel(
            "tycho_mempool_peer_in_curr_vsubset",
            "Current subset peer position (0 for not included)",
        ),
    ]
    return create_row("Mempool peers", metrics)


def mempool_storage() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_mempool_store_expand_anchor_history_time",
            "Adapter: expand anchor history into payload with DB",
        ),
        create_heatmap_panel(
            "tycho_mempool_store_set_committed_status_time",
            "Adapter: set anchor history as committed in DB",
        ),
        create_heatmap_panel(
            "tycho_mempool_store_insert_point_time",
            "Insert point with info and status",
        ),
        create_heatmap_panel(
            "tycho_mempool_store_set_status_time",
            "Set status",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_store_get_point_count", range_selector="$__interval"
            ),
            "Get point count (total at moment)",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_store_get_point_raw_count", range_selector="$__interval"
            ),
            "Get point raw count (total at moment)",
        ),
        create_heatmap_panel(
            "tycho_mempool_store_get_point_time",
            "Get point",
        ),
        create_heatmap_panel(
            "tycho_mempool_store_get_point_raw_time",
            "Get point raw",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_store_get_status_count", range_selector="$__interval"
            ),
            "Get status count (total at moment)",
        ),
        create_heatmap_panel(
            "tycho_mempool_store_get_status_time",
            "Get status",
        ),
        create_counter_panel(
            expr_sum_increase(
                "tycho_mempool_store_get_info_count", range_selector="$__interval"
            ),
            "Get info count (total at moment)",
        ),
        create_heatmap_panel(
            "tycho_mempool_store_get_info_time",
            "Get info",
        ),
        create_heatmap_panel(
            "tycho_mempool_store_clean_time",
            "Clean task",
        ),
    ]
    return create_row("Mempool storage", metrics)


def collator_execution_manager() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_collator_execute_ordinary_time", "Execute ordinary time"
        ),
        create_heatmap_panel(
            "tycho_collator_execute_ticktock_time", "Execute ticktock time"
        ),
    ]
    return create_row("collator: Execution Manager", metrics)


def allocator_stats() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "jemalloc_allocated_bytes", "Allocated Bytes", UNITS.BYTES_IEC
        ),
        create_gauge_panel("jemalloc_active_bytes", "Active Bytes", UNITS.BYTES_IEC),
        create_gauge_panel(
            "jemalloc_metadata_bytes", "Metadata Bytes", UNITS.BYTES_IEC
        ),
        create_gauge_panel(
            "jemalloc_resident_bytes", "Resident Bytes", UNITS.BYTES_IEC
        ),
        create_gauge_panel("jemalloc_mapped_bytes", "Mapped Bytes", UNITS.BYTES_IEC),
        create_gauge_panel(
            "jemalloc_retained_bytes", "Retained Bytes", UNITS.BYTES_IEC
        ),
        create_gauge_panel("jemalloc_dirty_bytes", "Dirty Bytes", UNITS.BYTES_IEC),
        create_gauge_panel(
            "jemalloc_fragmentation_bytes", "Fragmentation Bytes", UNITS.BYTES_IEC
        ),
    ]
    return create_row("Allocator Stats", metrics)


def rayon_stats() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_rayon_lifo_threads", "LIFO Threads", yaxis(UNITS.NUMBER_FORMAT)
        ),
        create_heatmap_panel(
            "tycho_rayon_fifo_threads", "FIFO Threads", yaxis(UNITS.NUMBER_FORMAT)
        ),
        create_heatmap_panel(
            "tycho_rayon_lifo_task_time", "LIFO Task Time", yaxis(UNITS.SECONDS)
        ),
        create_heatmap_panel(
            "tycho_rayon_fifo_task_time", "FIFO Task Time", yaxis(UNITS.SECONDS)
        ),
        create_heatmap_panel(
            "tycho_rayon_lifo_queue_time", "LIFO Queue Time", yaxis(UNITS.SECONDS)
        ),
        create_heatmap_panel(
            "tycho_rayon_fifo_queue_time", "FIFO Queue Time", yaxis(UNITS.SECONDS)
        ),
    ]
    return create_row("Rayon Stats", metrics)


def quic_network_panels() -> list[RowPanel]:
    """Panels for QUIC network performance monitoring"""
    common_labels = ['peer_id=~"$peer_id"', 'peer_addr=~"$remote_addr"']
    legend = "{{instance}} ->  {{peer_addr}}"
    by_labels = ["instance", "peer_addr"]

    def counter_with_defaults(metric, title, unit=UNITS.PACKETS_SEC):
        return create_counter_panel(
            metric,
            title,
            labels_selectors=common_labels,
            legend_format=legend,
            by_labels=by_labels,
            unit_format=unit,
        )

    def gauge_with_defaults(metric, title, unit=UNITS.PACKETS_SEC):
        return create_gauge_panel(
            metric, title, labels=common_labels, legend_format=legend, unit_format=unit
        )

    # Core network metrics
    network_perf_panels = [
        gauge_with_defaults(
            "tycho_network_connection_rtt_ms", "RTT", UNITS.MILLI_SECONDS
        ),
        gauge_with_defaults(
            "tycho_network_connection_cwnd", "Congestion Window", "packets"
        ),
        counter_with_defaults(
            "tycho_network_connection_invalid_messages", "Invalid Messages"
        ),
        counter_with_defaults(
            "tycho_network_connection_congestion_events", "Congestion Events"
        ),
        create_percent_panel(
            "tycho_network_connection_lost_packets",
            "tycho_network_connection_sent_packets",
            "Packet Loss Rate",
            label_selectors=common_labels,
        ),
    ]

    # Throughput metrics
    data_movement_panels = [
        counter_with_defaults(
            "tycho_network_connection_rx_bytes", "Bytes Received/s", UNITS.BYTES_IEC
        ),
        counter_with_defaults(
            "tycho_network_connection_tx_bytes", "Bytes Sent/s", UNITS.BYTES_IEC
        ),
    ]

    def frame_panel_pair(frame_type, display_name):
        return [
            counter_with_defaults(
                f"tycho_network_connection_rx_{frame_type}", f"RX {display_name}"
            ),
            counter_with_defaults(
                f"tycho_network_connection_tx_{frame_type}", f"TX {display_name}"
            ),
        ]

    protocol_panels = []
    for frame_type, name in [
        ("stream", "Stream Frames"),
        ("acks", "ACK Frames"),
        ("datagram", "Datagram Frames"),
        ("max_data", "Max Data Frames"),
        ("max_stream_data", "Max Stream Data"),
        ("ping", "Ping Frames"),
        ("crypto", "Crypto Handshake"),
    ]:
        protocol_panels.extend(frame_panel_pair(frame_type, name))

    error_panels = [
        counter_with_defaults(
            [
                "tycho_network_connection_rx_connection_close",
                "tycho_network_connection_tx_connection_close",
            ],
            "Connection Close Frames",
        ),
        counter_with_defaults(
            [
                "tycho_network_connection_rx_reset_stream",
                "tycho_network_connection_tx_reset_stream",
            ],
            "Reset Stream Frames",
        ),
        counter_with_defaults(
            [
                "tycho_network_connection_rx_stop_sending",
                "tycho_network_connection_tx_stop_sending",
            ],
            "Stop Sending Frames",
        ),
        counter_with_defaults(
            [
                "tycho_network_connection_rx_data_blocked",
                "tycho_network_connection_tx_data_blocked",
            ],
            "Data Blocked Frames",
        ),
        counter_with_defaults(
            [
                "tycho_network_connection_rx_stream_data_blocked",
                "tycho_network_connection_tx_stream_data_blocked",
            ],
            "Stream Data Blocked Frames",
        ),
    ]

    return [
        create_row("quinn: Network Performance", network_perf_panels),
        create_row("quinn: Data Movement", data_movement_panels),
        create_row("quinn: Protocol Operations", protocol_panels),
        create_row("quinn: Error Conditions", error_panels),
    ]


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
            template(
                name="workchain",
                query="label_values(tycho_do_collate_block_time_diff,workchain)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="partition",
                query="label_values(tycho_do_collate_processed_upto_int_ranges,par_id)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="kind",
                query="label_values(tycho_mempool_verifier_verify,kind)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="method",
                query="label_values(tycho_jrpc_request_time_bucket,method)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="peer_id",
                query="label_values(tycho_network_connection_rtt_ms, peer_id)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="remote_addr",
                query="label_values(tycho_network_connection_rtt_ms, addr)",
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
    refresh="30s",
    panels=[
        blockchain_stats(),
        core_bc(),
        core_block_strider(),
        core_blockchain_rpc(),
        storage(),
        collator_params_metrics(),
        collation_metrics(),
        collator_execution_metrics(),
        collator_message_metrics(),
        collator_queue_metrics(),
        collator_special_transactions_metrics(),
        collator_time_metrics(),
        collator_wu_metrics(),
        collator_core_operations_metrics(),
        collator_finalize_block(),
        collator_execution_manager(),
        collator_state_adapter_metrics(),
        collator_misc_operations_metrics(),
        collator_commit_block_metrics(),
        validator(),
        mempool_rounds(),
        mempool_payload_rates(),
        mempool_engine_rates(),
        mempool_engine(),
        mempool_intercom(),
        mempool_peers(),
        mempool_storage(),
        net_traffic(),
        net_conn_manager(),
        net_request_handler(),
        net_peer(),
        net_dht(),
        *quic_network_panels(),
        allocator_stats(),
        rayon_stats(),
        jrpc(),
        jrpc_timings(),
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
