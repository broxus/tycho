import sys
from typing import Union, List

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
    expr_sum_rate,
    heatmap_panel,
    yaxis,
    expr_operator,
)


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
    expr: Union[str, List[Union[str, Expr]]],
    title: str,
    unit_format=UNITS.NUMBER_FORMAT,
    labels=[],
) -> Panel:
    if isinstance(expr, str):
        expr = [Expr(metric=expr, label_selectors=labels)]
    elif isinstance(expr, list):
        expr = [
            Expr(metric=e, label_selectors=labels) if isinstance(e, str) else e
            for e in expr
        ]
    else:
        raise TypeError(
            "expr must be a string, a list of strings, or a list of Expr objects."
        )

    legend_format = generate_legend_format(labels)
    targets = [target(e, legend_format=legend_format) for e in expr]

    return timeseries_panel(
        title=title,
        targets=targets,
        unit=unit_format,
    )


def create_counter_panel(
    expr: Union[str, List[Union[str, Expr]]],
    title: str,
    unit_format: str = UNITS.NUMBER_FORMAT,
    labels_selectors: List[str] = [],
    legend_format: str | None = None,
    by_labels: list[str] = ["instance"],
) -> Panel:
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
                    expr_sum_rate(e, label_selectors=labels_selectors),
                    legend_format=legend_format,
                )
                for e in expr
            ]
        elif all(isinstance(e, Expr) for e in expr):
            targets = [target(e, legend_format=legend_format) for e in expr]
        else:
            raise ValueError("List elements must be all strings or all Expr objects.")
    else:
        raise TypeError(
            "expr must be a string, a list of strings, or a list of Expr objects."
        )

    return timeseries_panel(
        title=title,
        targets=targets,
        unit=unit_format,
    )


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
    )


def create_heatmap_quantile_panel(
    metric_name: str,
    title: str,
    unit_format=UNITS.NUMBER_FORMAT,
) -> Panel:
    return timeseries_panel(
        title=title,
        targets=[
            target(
                Expr(metric_name, label_selectors=['quantile="0.95"']),
                legend_format="{{instance}}",
            )
        ],
        unit=unit_format,
    )


def create_row(name, metrics) -> RowPanel:
    layout = Layout(name)
    for i in range(0, len(metrics), 2):
        chunk = metrics[i : i + 2]
        layout.row(chunk)
    return layout.row_panel


def core_bc() -> RowPanel:
    metrics = [
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
            "tycho_bc_total_gas_used", "Total gas used per block"
        ),
        create_heatmap_quantile_panel(
            "tycho_bc_in_msg_count", "Number of inbound messages per block"
        ),
        create_heatmap_quantile_panel(
            "tycho_bc_out_msg_count", "Number of outbound messages per block"
        ),
        create_heatmap_quantile_panel(
            "tycho_bc_out_in_msg_ratio", "Out/In message ratio per block"
        ),
        create_heatmap_quantile_panel(
            "tycho_bc_out_msg_acc_ratio",
            "Out message/Account ratio per block",
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
    ]
    return create_row("network: Traffic", metrics)


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
            "tycho_net_in_datagrams_total", "Number of incoming datagrams over time"
        ),
        create_gauge_panel(
            "tycho_net_req_handlers", "Current number of incoming request handlers"
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
            "tycho_core_subscriber_handle_state_time",
            "Total time to handle state by all subscribers",
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
            "tycho_storage_load_cell_time", "Time to load cell from storage"
        ),
        create_heatmap_panel(
            "tycho_storage_get_cell_from_rocksdb_time", "Time to load cell from RocksDB"
        ),
        create_heatmap_quantile_panel(
            "tycho_storage_store_block_data_size", "Block data size", UNITS.BYTES
        ),
        create_heatmap_quantile_panel(
            "tycho_storage_cell_count", "Number of new cells from merkle update"
        ),
        create_heatmap_panel(
            "tycho_storage_state_update_time", "Time to write state update to rocksdb"
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
                            metric="tycho_do_collate_block_seqno",
                            label_selectors=['workchain="-1"'],
                        ),
                        "- on(instance, job)",
                        Expr("tycho_gc_states_seqno"),
                    ),
                    legend_format="{{instance}}",
                )
            ],
            unit="Blocks",
            title="GC lag",
        ),
    ]
    return create_row("block strider: Core Metrics", metrics)


def jrpc() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_rpc_state_update_time", "Time to update RPC state on block"
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
            "Params: group vertical size" "Params: group vertical size limit",
        ),
    ]
    return create_row("collator: Parameters", metrics)


def block_metrics() -> RowPanel:
    metrics = [
        create_counter_panel(
            "tycho_do_collate_blocks_count",
            "Blocks rate",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_blocks_with_limits_reached_count",
            "Number of blocks with limits reached",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_tx_total",
            "Number of transactions over time",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_block_seqno",
            "Block seqno",
            labels=['workchain=~"$workchain"'],
        ),
    ]
    return create_row("collator: Block Metrics", metrics)


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
    ]
    return create_row("collator: Execution Metrics", metrics)


def collator_message_metrics() -> RowPanel:
    metrics = [
        create_counter_panel(
            "tycho_do_collate_msgs_exec_count_all",
            "All executed msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_collator_ext_msgs_imported_count",
            "Imported Ext msgs count from mempool",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_read_count_ext",
            "Read Ext msgs count",
            labels_selectors=['workchain=~"$workchain"'],
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
            "tycho_do_collate_ext_msgs_expired_count",
            "Ext msgs expired count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_read_count_int",
            "Read Int msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_exec_count_int",
            "Executed Int msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_new_msgs_created_count",
            "Created NewInt msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_new_msgs_inserted_to_iterator_count",
            "Inserted to iterator NewInt msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_read_count_new_int",
            "Read NewInt msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
        create_counter_panel(
            "tycho_do_collate_msgs_exec_count_new_int",
            "Executed NewInt msgs count",
            labels_selectors=['workchain=~"$workchain"'],
        ),
    ]
    return create_row("collator: Message Metrics", metrics)


def collator_queue_metrics() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "tycho_session_iterator_messages_all",
            "Number of internals in the iterator",
            labels=['workchain=~"$workchain"'],
        ),
        create_gauge_panel(
            "tycho_do_collate_int_msgs_queue_calc", "Calculated Internal queue len"
        ),
        create_counter_panel(
            "tycho_do_collate_int_enqueue_count", "Enqueued int msgs count"
        ),
        create_counter_panel(
            "tycho_do_collate_int_dequeue_count", "Dequeued int msgs count"
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
        create_heatmap_panel(
            "tycho_do_collate_from_prev_block_time",
            "Time elapsed from prev block",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_overhead_time",
            "Collation flow overhead",
            labels=['workchain=~"$workchain"'],
        ),
    ]
    return create_row("collator: Time diffs", metrics)


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
            "Create message queue diff",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_do_collate_apply_queue_diff_time",
            "Apply message queue diff",
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
            "tycho_collator_update_mc_data_time",
            "Update mc data",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_import_next_anchor_time",
            "Import next anchor time",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_import_next_anchors_on_init_time",
            "Import anchors on init time",
            labels=['workchain=~"$workchain"'],
        ),
        create_heatmap_panel(
            "tycho_collator_try_collate_next_master_block_time",
            "Try collate next master block",
        ),
        create_heatmap_panel(
            "tycho_collator_try_collate_next_shard_block_without_do_collate_time",
            "Try collate next shard block",
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
            "tycho_collator_refresh_collation_sessions_time",
            "Refresh collation sessions",
        ),
        create_heatmap_panel(
            "tycho_collator_process_collated_block_candidate_time",
            "Process collated block candidate",
        ),
        create_heatmap_panel(
            "tycho_collator_update_last_collated_chain_time_and_check_should_collate_mc_block_time",
            "Update last collated chain time and check should collate mc block",
        ),
        create_heatmap_panel(
            "tycho_collator_enqueue_mc_block_collation_time",
            "Enqueue mc block collation",
        ),
        create_heatmap_panel(
            "tycho_collator_process_validated_block_time", "Process validated block"
        ),
        create_heatmap_panel(
            "tycho_collator_process_valid_master_block_time",
            "Process valid master block",
        ),
        create_heatmap_panel(
            "tycho_collator_extract_master_block_subgraph_time",
            "Extract master block subgraph",
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


def collator_sync_metrics() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_collator_send_blocks_to_sync_time", "send blocks to sync total"
        ),
        create_heatmap_panel(
            "tycho_collator_build_block_stuff_for_sync_time",
            "send blocks to sync: build stuff",
        ),
        create_heatmap_panel(
            "tycho_collator_sync_block_stuff_time", "send blocks to sync: sync"
        ),
        create_heatmap_panel(
            "tycho_collator_send_blocks_to_sync_commit_diffs_time",
            "send blocks to sync: commit diffs",
        ),
    ]
    return create_row("collator: Sync Metrics", metrics)


def collator_adapter_metrics() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_collator_adapter_on_block_accepted_time", "on_block_accepted"
        ),
        create_heatmap_panel(
            "tycho_collator_adapter_on_block_accepted_ext_time",
            "on_block_accepted_external",
        ),
    ]
    return create_row("collator: Adapter Metrics", metrics)


def mempool() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "tycho_mempool_last_anchor_round",
            "Adapter: last anchor round",
        ),
        create_gauge_panel(
            "tycho_mempool_engine_current_round",
            "Engine: current round",
        ),
        # == Mempool adapter == #
        create_counter_panel(
            "tycho_mempool_externals_count_total",
            "Adapter: unique externals count",
        ),
        create_counter_panel(
            "tycho_mempool_externals_bytes_total",
            "Adapter: unique externals size",
            unit_format=UNITS.BYTES,
        ),
        create_counter_panel(
            "tycho_mempool_duplicates_count_total",
            "Adapter: removed duplicate externals count",
        ),
        create_counter_panel(
            "tycho_mempool_duplicates_bytes_total",
            "Adapter: removed duplicate externals size",
            unit_format=UNITS.BYTES,
        ),
        create_counter_panel(
            "tycho_mempool_evicted_externals_count",
            "Input buffer: evicted externals count",
        ),
        create_counter_panel(
            "tycho_mempool_evicted_externals_size",
            "Input buffer: evicted externals size",
            unit_format=UNITS.BYTES,
        ),
        # == Engine own point == #
        create_counter_panel(
            "tycho_mempool_point_payload_count",
            "Engine: points payload count",
        ),
        create_counter_panel(
            "tycho_mempool_point_payload_bytes",
            "Engine: points payload size",
            unit_format=UNITS.BYTES,
        ),
        create_counter_panel(
            "tycho_mempool_points_produced",
            "Engine: produced points",
        ),
        create_counter_panel(
            "tycho_mempool_points_no_proof_produced",
            "Engine: produced points without proof",
        ),
        # == Engine == #
        create_counter_panel(
            "tycho_mempool_engine_rounds_skipped",
            "Engine: skipped rounds",
        ),
        create_heatmap_panel(
            "tycho_mempool_engine_round_time",
            "Engine: round duration",
        ),
        create_counter_panel(
            "tycho_mempool_engine_produce_skipped",
            "Engine: points to produce skipped",
        ),
        create_heatmap_panel(
            "tycho_mempool_engine_produce_time",
            "Engine: produce point task duration",
        ),
        # == Engine commit == #
        create_counter_panel(
            "tycho_mempool_commit_anchors",
            "Engine: committed anchors",
        ),
        create_heatmap_panel(
            "tycho_mempool_engine_commit_time",
            "Engine: commit duration",
        ),
        create_gauge_panel(
            "tycho_mempool_commit_latency_rounds",
            "Engine: committed anchor rounds latency (max over batch)",
        ),
        create_heatmap_panel(
            "tycho_mempool_commit_anchor_latency_time",
            "Engine: committed anchor time latency (min over batch)",
        ),
    ]
    return create_row("Mempool", metrics)


def mempool_components() -> RowPanel:
    metrics = [
        # == Verifier == #
        create_counter_panel(
            "tycho_mempool_verifier_verify",
            "Verifier: verify() errors",
            labels_selectors=['kind=~"$kind"'],
        ),
        create_heatmap_panel(
            "tycho_mempool_verifier_verify_time",
            "Verifier: verify() point structure and author's sig",
        ),
        create_counter_panel(
            "tycho_mempool_verifier_validate",
            "Verifier: validate() errors and warnings",
            labels_selectors=['kind=~"$kind"'],
        ),
        create_heatmap_panel(
            "tycho_mempool_verifier_validate_time",
            "Verifier: validate() point dependencies in DAG and all-1 sigs",
        ),
        # == Download tasks - multiple per round == #
        create_counter_panel(
            "tycho_mempool_download_task_count",
            "Downloader: tasks (unique point id)",
        ),
        create_heatmap_panel(
            "tycho_mempool_download_task_time", "Downloader: tasks duration"
        ),
        # FIXME next one needs max value over collection period, but no `gauge.set_max()`
        create_gauge_panel(
            "tycho_mempool_download_depth_rounds",
            "Downloader: point depth (max rounds from current) #fixme",
        ),
        create_counter_panel(
            "tycho_mempool_download_not_found_responses",
            "Downloader: received None in response",
        ),
        create_counter_panel(
            "tycho_mempool_download_aborted_on_exit_count",
            "Downloader: queries aborted (on task completion)",
        ),
        create_counter_panel(
            "tycho_mempool_download_query_failed_count",
            "Downloader: queries network error",
        ),
        # == Network tasks - multiple per round == #
        create_heatmap_panel(
            "tycho_mempool_broadcast_query_dispatcher_time",
            "Dispatcher: Broadcast send",
        ),
        create_heatmap_panel(
            "tycho_mempool_broadcast_query_responder_time",
            "Responder: Broadcast accept",
        ),
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
            "tycho_mempool_signature_query_responder_pong_time",
            "Responder: Signature send: no point or try later",
        ),
        create_heatmap_panel(
            "tycho_mempool_download_query_responder_some_time",
            "Responder: Download send: Some(point)",
        ),
        create_heatmap_panel(
            "tycho_mempool_download_query_responder_none_time",
            "Responder: Download send: None",
        ),
    ]
    return create_row("Mempool components", metrics)


def collator_execution_manager() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_collator_execute_ordinary_time",
            "Execute ordinary time",
            yaxis(UNITS.SECONDS),
        ),
    ]
    return create_row("collator: Execution Manager", metrics)


def allocator_stats() -> RowPanel:
    metrics = [
        create_gauge_panel("jemalloc_allocated_bytes", "Allocated Bytes", UNITS.BYTES),
        create_gauge_panel("jemalloc_active_bytes", "Active Bytes", UNITS.BYTES),
        create_gauge_panel("jemalloc_metadata_bytes", "Metadata Bytes", UNITS.BYTES),
        create_gauge_panel("jemalloc_resident_bytes", "Resident Bytes", UNITS.BYTES),
        create_gauge_panel("jemalloc_mapped_bytes", "Mapped Bytes", UNITS.BYTES),
        create_gauge_panel("jemalloc_retained_bytes", "Retained Bytes", UNITS.BYTES),
        create_gauge_panel("jemalloc_dirty_bytes", "Dirty Bytes", UNITS.BYTES),
        create_gauge_panel(
            "jemalloc_fragmentation_bytes", "Fragmentation Bytes", UNITS.BYTES
        ),
    ]
    return create_row("Allocator Stats", metrics)


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
        ]
    )


dashboard = Dashboard(
    "Tycho Node Metrics",
    templating=templates(),
    refresh="5s",
    panels=[
        core_bc(),
        core_block_strider(),
        collator_params_metrics(),
        block_metrics(),
        collator_execution_metrics(),
        collator_message_metrics(),
        collator_queue_metrics(),
        collator_time_metrics(),
        collator_core_operations_metrics(),
        collator_misc_operations_metrics(),
        collator_special_transactions_metrics(),
        collator_sync_metrics(),
        collator_adapter_metrics(),
        collator_finalize_block(),
        collator_execution_manager(),
        mempool(),
        mempool_components(),
        net_traffic(),
        net_conn_manager(),
        net_request_handler(),
        net_peer(),
        net_dht(),
        allocator_stats(),
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
