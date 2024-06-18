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
    expr_sum_rate,
    heatmap_panel,
    yaxis,
)


def heatmap_color_warm() -> HeatmapColor:
    return HeatmapColor()


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
    ]
    return create_row("Core Block Strider", metrics)


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
            "tycho_collator_finalize_block_time", "Total time to finalize block"
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_build_account_blocks_time", "Build account blocks"
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_build_in_msgs_time", "Build InMsgDescr"
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_build_out_msgs_time",
            "Build OutMsgDescr",
        ),
        create_heatmap_panel(
            "tycho_collator_finish_build_mc_state_extra_time", "Build McStateExtra"
        ),
        create_heatmap_panel(
            "tycho_collator_finalize_build_state_update_time", "Compute MerkleUpdate"
        ),
        create_heatmap_panel("tycho_collator_finalize_build_block_time", "Build Block"),
        create_heatmap_panel(
            "tycho_collator_finalize_build_new_state_time", "Build State"
        ),
    ]
    return create_row("Finalize Block", metrics)


def collator_do_collate() -> RowPanel:
    metrics = [
        create_counter_panel(
            "tycho_do_collate_tx_total",
            "Number of transactions over time",
        ),
        create_heatmap_panel(
            "tycho_do_collate_block_diff_time", "Block time diff"
        ),
        create_heatmap_panel(
            "tycho_do_collate_from_prev_block_time", "Time elapsed from prev block"
        ),
        create_heatmap_panel(
            "tycho_do_collate_overhead_time", "Collation management overhead"
        ),
        create_heatmap_panel("tycho_do_collate_total_time", "Total collation time"),
        create_heatmap_panel("tycho_do_collate_prepare_time", "Collation prepare time"),
        create_heatmap_panel(
            "tycho_do_collate_init_iterator_time", "Init iterator time"
        ),
        create_heatmap_panel("tycho_do_collate_execute_time", "Execution time"),
        create_heatmap_panel(
            "tycho_do_collate_execute_tick_time", "Execute Tick special transactions"
        ),
        create_heatmap_panel(
            "tycho_do_collate_execute_tock_time", "Execute Tock special transactions"
        ),
        create_heatmap_panel(
            "tycho_do_collate_fill_msgs_total_time", "Fill messages time"
        ),
        create_heatmap_panel(
            "tycho_do_collate_process_msgs_total_time", "Execute messages time"
        ),
        create_heatmap_panel(
            "tycho_do_collate_process_txs_total_time",
            "Process transactions time",
        ),
        create_heatmap_panel(
            "tycho_do_collate_apply_queue_diff_time", "Apply message queue diff"
        ),
        create_heatmap_panel(
            "tycho_do_collate_handle_block_candidate_time", "Handle block candidate"
        ),
        create_heatmap_panel("tycho_collator_update_mc_data_time", "update mc data"),
        create_heatmap_panel(
            "tycho_collator_import_next_anchor_time", "import next anchor time"
        ),
        create_heatmap_panel(
            "tycho_collator_try_collate_next_master_block_time",
            "try collate next master block",
        ),
        create_heatmap_panel(
            "tycho_collator_try_collate_next_shard_block_without_do_collate_time",
            "try collate next shard block",
        ),
        create_heatmap_panel(
            "tycho_collator_refresh_collation_sessions_time",
            "refresh collation sessions",
        ),
        create_heatmap_panel(
            "tycho_collator_process_collated_block_candidate_time",
            "process collated block candidate",
        ),
        create_heatmap_panel(
            "tycho_collator_update_last_collated_chain_time_and_check_should_collate_mc_block_time",
            "update last collated chain time and check should collate mc block",
        ),
        create_heatmap_panel(
            "tycho_collator_enqueue_mc_block_collation_time",
            "enqueue mc block collation",
        ),
        create_heatmap_panel(
            "tycho_collator_process_validated_block_time", "process validated block"
        ),
        create_heatmap_panel(
            "tycho_collator_process_valid_master_block_time", "process valid master block"
        ),
        create_heatmap_panel(
            "tycho_collator_extract_master_block_subgraph_time", "extract master block subgraph"
        ),
        create_heatmap_panel(
            "tycho_collator_send_blocks_to_sync_time", "send blocks to sync total"
        ),
        create_heatmap_panel(
            "tycho_collator_build_block_stuff_for_sync_time", "send blocks to sync: build stuff"
        ),
        create_heatmap_panel(
            "tycho_collator_sync_block_stuff_time", "send blocks to sync: sync"
        ),
        create_heatmap_panel(
            "tycho_collator_send_blocks_to_sync_commit_diffs_time", "send blocks to sync: commit diffs"
        ),
        create_heatmap_panel(
            "tycho_collator_adapter_on_block_accepted_time", "on_block_accepted"
        ),
        create_heatmap_panel(
            "tycho_collator_adapter_on_block_accepted_ext_time",
            "on_block_accepted_external",
        ),
    ]
    return create_row("Collator Do Collate", metrics)


def collator_execution_manager() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_message_execution_time",
            "Message execution time",
            yaxis(UNITS.SECONDS),
        ),
    ]
    return create_row("Collator Execution Manager", metrics)


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
        core_bc(),
        core_block_strider(),
        collator_do_collate(),
        collator_finalize_block(),
        collator_execution_manager(),
        net_conn_manager(),
        net_request_handler(),
        net_peer(),
        net_dht(),
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
