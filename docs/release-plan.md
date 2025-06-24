# Release plan and instructions

## 0.2.11

- updated nodes will produce mismatched blocks, network will run after 2/3n+1 been updated
- before update:
- on update:
  - update node config, removing all fields from storage except `root_dir`, `rocksdb_enable_metrics`, and `rocksdb_lru_capacity`
  - update node config, adding

```
"core_storage": {
  "cells_cache_size": "4096 MB",
  "archives_gc": {},
  "blocks_gc": {
    "type": "BeforeSafeDistance",
    "safe_distance": 1000,
    "min_interval": "1m"
  },
  "states_gc": {}
}
```

- after update:
  - set blockchain config params.28.work_units_params

```
    {
      "prepare": {
        "fixed_part": 1000000,
        "msgs_stats": 0,
        "remaning_msgs_stats": 0,
        "read_ext_msgs": 500,
        "read_int_msgs": 3000,
        "read_new_msgs": 1000,
        "add_to_msg_groups": 150
      },
      "execute": {
        "prepare": 57000,
        "execute": 10000,
        "execute_err": 0,
        "execute_delimiter": 1000,
        "serialize_enqueue": 80,
        "serialize_dequeue": 80,
        "insert_new_msgs": 80,
        "subgroup_size": 16
      },
      "finalize": {
        "build_transactions": 200,
        "build_accounts": 1500,
        "build_in_msg": 150,
        "build_out_msg": 150,
        "serialize_min": 2500000,
        "serialize_accounts": 3000,
        "serialize_msg": 3000,
        "state_update_min": 1000000,
        "state_update_accounts": 4000,
        "state_update_msg": 0,
        "create_diff": 1000,
        "serialize_diff": 0,
        "apply_diff": 2000,
        "diff_tail_len": 0
      }
    }
```

## 0.2.10

- before update:
  - externals and internals queues must be empty, stop rpc before update
- after update:
  - set blockchain config params.28.msgs_exec_params

```
    {
      "buffer_limit": 10000,
      "group_limit": 100,
      "group_vert_size": 10,
      "externals_expire_timeout": 58,
      "open_ranges_limit": 20,
      "par_0_int_msgs_count_limit": 10000000,
      "par_0_ext_msgs_count_limit": 10000000,
      "group_slots_fractions": {
        "0": 80,
        "1": 10
      },
      "range_messages_limit": 10000
    }
```

## 0.2.9

...

## 0.2.8

...

## 0.2.7 (MANDATORY)

- before update:
  - internals queue must be empty, stop rpc before update
  - stop archive and indexer nodes before update
  - set blockchain config params.43

```
  {
    "max_msg_bits": 2097152,
    "max_msg_cells": 8192,
    "max_library_cells": 1000,
    "max_vm_data_depth": 512,
    "max_ext_msg_size": 65535,
    "max_ext_msg_depth": 512,
    "max_acc_state_cells": 65536,
    "max_acc_state_bits": 67043328,
    "max_acc_public_libraries": 256,
    "defer_out_queue_size_limit": 256
  }
```

- on update:
  - init new genesis in mempool
