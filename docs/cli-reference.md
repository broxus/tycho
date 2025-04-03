# Command-Line Help for `tycho`

This document contains the help content for the `tycho` command-line program.

**Command Overview:**

* [`tycho`↴](#tycho)
* [`tycho init`↴](#tycho-init)
* [`tycho init config`↴](#tycho-init-config)
* [`tycho init validator`↴](#tycho-init-validator)
* [`tycho init systemd`↴](#tycho-init-systemd)
* [`tycho node`↴](#tycho-node)
* [`tycho node run`↴](#tycho-node-run)
* [`tycho node status`↴](#tycho-node-status)
* [`tycho node ping`↴](#tycho-node-ping)
* [`tycho node get-account`↴](#tycho-node-get-account)
* [`tycho node get-neighbours`↴](#tycho-node-get-neighbours)
* [`tycho node find-archive`↴](#tycho-node-find-archive)
* [`tycho node list-archives`↴](#tycho-node-list-archives)
* [`tycho node dump-archive`↴](#tycho-node-dump-archive)
* [`tycho node list-blocks`↴](#tycho-node-list-blocks)
* [`tycho node dump-block`↴](#tycho-node-dump-block)
* [`tycho node dump-proof`↴](#tycho-node-dump-proof)
* [`tycho node dump-queue-diff`↴](#tycho-node-dump-queue-diff)
* [`tycho node gc-archives`↴](#tycho-node-gc-archives)
* [`tycho node gc-blocks`↴](#tycho-node-gc-blocks)
* [`tycho node gc-states`↴](#tycho-node-gc-states)
* [`tycho node mem-profiler`↴](#tycho-node-mem-profiler)
* [`tycho node mem-profiler start`↴](#tycho-node-mem-profiler-start)
* [`tycho node mem-profiler stop`↴](#tycho-node-mem-profiler-stop)
* [`tycho node mem-profiler dump`↴](#tycho-node-mem-profiler-dump)
* [`tycho tool`↴](#tycho-tool)
* [`tycho tool gen-dht`↴](#tycho-tool-gen-dht)
* [`tycho tool gen-key`↴](#tycho-tool-gen-key)
* [`tycho tool gen-zerostate`↴](#tycho-tool-gen-zerostate)
* [`tycho tool gen-account`↴](#tycho-tool-gen-account)
* [`tycho tool gen-account wallet`↴](#tycho-tool-gen-account-wallet)
* [`tycho tool gen-account multisig`↴](#tycho-tool-gen-account-multisig)
* [`tycho tool gen-account giver`↴](#tycho-tool-gen-account-giver)
* [`tycho tool bc`↴](#tycho-tool-bc)
* [`tycho tool bc get-param`↴](#tycho-tool-bc-get-param)
* [`tycho tool bc set-param`↴](#tycho-tool-bc-set-param)
* [`tycho tool bc set-master-key`↴](#tycho-tool-bc-set-master-key)
* [`tycho tool bc set-elector-code`↴](#tycho-tool-bc-set-elector-code)
* [`tycho tool bc set-config-code`↴](#tycho-tool-bc-set-config-code)
* [`tycho elect`↴](#tycho-elect)
* [`tycho elect run`↴](#tycho-elect-run)
* [`tycho elect once`↴](#tycho-elect-once)
* [`tycho elect recover`↴](#tycho-elect-recover)
* [`tycho elect withdraw`↴](#tycho-elect-withdraw)
* [`tycho elect get-state`↴](#tycho-elect-get-state)
* [`tycho util`↴](#tycho-util)
* [`tycho util markdown-help`↴](#tycho-util-markdown-help)

## `tycho`

Tycho Node

**Usage:** `tycho [OPTIONS] <COMMAND>`

###### **Subcommands:**

* `init` — Create a node environment or reinitialize an existing one
* `node` — Manage the node
* `tool` — Work with blockchain stuff
* `elect` — Participate in validator elections
* `util` — Work with shell environment

###### **Options:**

* `--home <HOME>` — Directory for config and keys

  Default value: `/tmp/.tycho-ci`



## `tycho init`

Create a node environment or reinitialize an existing one

**Usage:** `tycho init [OPTIONS] --global-config <GLOBAL_CONFIG>
       init [OPTIONS] <COMMAND>`

###### **Subcommands:**

* `config` — Generate a default node config
* `validator` — Generate validator keys and wallet
* `systemd` — Generate systemd services

###### **Options:**

* `-b`, `--binary <BINARY>` — Path to the `tycho` binary

  Default value: `/tmp/tycho-bin`
* `--validator` — Whether to init as a validator
* `--stake <STAKE>` — Validator stake per round
* `--systemd` — Whether to create a systemd services
* `-g`, `--global-config <GLOBAL_CONFIG>` — Path or URL of the global config



## `tycho init config`

Generate a default node config

**Usage:** `tycho init config [OPTIONS] [OUTPUT]`

###### **Arguments:**

* `<OUTPUT>` — Custom path to the output file. Default: `$TYCHO_HOME/config.json`

###### **Options:**

* `-f`, `--force` — Overwrite the existing config



## `tycho init validator`

Generate validator keys and wallet

**Usage:** `tycho init validator --stake <STAKE>`

###### **Options:**

* `--stake <STAKE>` — Validator stake per round



## `tycho init systemd`

Generate systemd services

**Usage:** `tycho init systemd [OPTIONS]`

###### **Options:**

* `-b`, `--binary <BINARY>` — Path to the `tycho` binary

  Default value: `/tmp/tycho-bin`



## `tycho node`

Manage the node

**Usage:** `tycho node <COMMAND>`

###### **Subcommands:**

* `run` — Run a Tycho node
* `status` — Get node status
* `ping` — Ping the control server
* `get-account` — Get account state from the node
* `get-neighbours` — Get list of all known public overlay neighbours
* `find-archive` — Get archive info from the node
* `list-archives` — Fetch the list of all stored archive ids
* `dump-archive` — Dump the archive from the node
* `list-blocks` — Fetch the list of all stored block ids
* `dump-block` — Download a block from the node
* `dump-proof` — Dump a block proof from the node
* `dump-queue-diff` — Dump a queue diff from the node
* `gc-archives` — Trigger a garbage collection of archives
* `gc-blocks` — Trigger a garbage collection of blocks
* `gc-states` — Trigger a garbage collection of states
* `mem-profiler` — Manage memory profiler



## `tycho node run`

Run a Tycho node

**Usage:** `tycho node run [OPTIONS]`

###### **Options:**

* `--config <CONFIG>` — Path to the node config. Default: `$TYCHO_HOME/config.json`
* `--global-config <GLOBAL_CONFIG>` — Path to the global config. Default: `$TYCHO_HOME/global-config.json`
* `--keys <KEYS>` — Path to the node keys. Default: `$TYCHO_HOME/node_keys.json`
* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--logger-config <LOGGER_CONFIG>` — Path to the logger config
* `--import-zerostate <IMPORT_ZEROSTATE>` — List of zerostate files to import



## `tycho node status`

Get node status

**Usage:** `tycho node status [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`



## `tycho node ping`

Ping the control server

**Usage:** `tycho node ping [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`



## `tycho node get-account`

Get account state from the node

**Usage:** `tycho node get-account [OPTIONS] --addr <ADDR>`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `-a`, `--addr <ADDR>` — Account address
* `-p`, `--parse` — Parse the account state



## `tycho node get-neighbours`

Get list of all known public overlay neighbours

**Usage:** `tycho node get-neighbours [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `-h`, `--human-readable`
* `--help`

  Possible values: `true`, `false`




## `tycho node find-archive`

Get archive info from the node

**Usage:** `tycho node find-archive [OPTIONS] --seqno <SEQNO>`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--seqno <SEQNO>` — masterchain block seqno



## `tycho node list-archives`

Fetch the list of all stored archive ids

**Usage:** `tycho node list-archives [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `-h`, `--human-readable`
* `--help`

  Possible values: `true`, `false`




## `tycho node dump-archive`

Dump the archive from the node

**Usage:** `tycho node dump-archive [OPTIONS] --seqno <SEQNO> <OUTPUT>`

###### **Arguments:**

* `<OUTPUT>` — path to the output file

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--seqno <SEQNO>` — masterchain block seqno
* `-d`, `--decompress` — decompress the downloaded archive



## `tycho node list-blocks`

Fetch the list of all stored block ids

**Usage:** `tycho node list-blocks [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `-h`, `--human-readable`
* `--help`

  Possible values: `true`, `false`




## `tycho node dump-block`

Download a block from the node

**Usage:** `tycho node dump-block [OPTIONS] --block-id <BLOCK_ID> <OUTPUT>`

###### **Arguments:**

* `<OUTPUT>` — path to the output file

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `-b`, `--block-id <BLOCK_ID>` — full block ID



## `tycho node dump-proof`

Dump a block proof from the node

**Usage:** `tycho node dump-proof [OPTIONS] --block-id <BLOCK_ID> <OUTPUT>`

###### **Arguments:**

* `<OUTPUT>` — path to the output file

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `-b`, `--block-id <BLOCK_ID>` — full block ID



## `tycho node dump-queue-diff`

Dump a queue diff from the node

**Usage:** `tycho node dump-queue-diff [OPTIONS] --block-id <BLOCK_ID> <OUTPUT>`

###### **Arguments:**

* `<OUTPUT>` — path to the output file

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `-b`, `--block-id <BLOCK_ID>` — full block ID



## `tycho node gc-archives`

Trigger a garbage collection of archives

**Usage:** `tycho node gc-archives [OPTIONS] <--seqno <SEQNO>|--distance <DISTANCE>>`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--seqno <SEQNO>` — Triggers GC for the specified MC block seqno
* `--distance <DISTANCE>` — Triggers GC for the MC block seqno relative to the latest MC block



## `tycho node gc-blocks`

Trigger a garbage collection of blocks

**Usage:** `tycho node gc-blocks [OPTIONS] <--seqno <SEQNO>|--distance <DISTANCE>>`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--seqno <SEQNO>` — Triggers GC for the specified MC block seqno
* `--distance <DISTANCE>` — Triggers GC for the MC block seqno relative to the latest MC block



## `tycho node gc-states`

Trigger a garbage collection of states

**Usage:** `tycho node gc-states [OPTIONS] <--seqno <SEQNO>|--distance <DISTANCE>>`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--seqno <SEQNO>` — Triggers GC for the specified MC block seqno
* `--distance <DISTANCE>` — Triggers GC for the MC block seqno relative to the latest MC block



## `tycho node mem-profiler`

Manage memory profiler

**Usage:** `tycho node mem-profiler <COMMAND>`

###### **Subcommands:**

* `start` — Start the memory profiler
* `stop` — Stop the memory profiler
* `dump` — Dump the memory profiler data



## `tycho node mem-profiler start`

Start the memory profiler

**Usage:** `tycho node mem-profiler start [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`



## `tycho node mem-profiler stop`

Stop the memory profiler

**Usage:** `tycho node mem-profiler stop [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`



## `tycho node mem-profiler dump`

Dump the memory profiler data

**Usage:** `tycho node mem-profiler dump [OPTIONS] <OUTPUT>`

###### **Arguments:**

* `<OUTPUT>` — path to the output file

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`



## `tycho tool`

Work with blockchain stuff

**Usage:** `tycho tool <COMMAND>`

###### **Subcommands:**

* `gen-dht` — Generate a DHT entry for a node
* `gen-key` — Generate a new key pair
* `gen-zerostate` — Generate a zero state for a network
* `gen-account` — Generate an account state
* `bc` — Blockchain stuff



## `tycho tool gen-dht`

Generate a DHT entry for a node

**Usage:** `tycho tool gen-dht [OPTIONS] <ADDR>...`

###### **Arguments:**

* `<ADDR>` — a list of node addresses

###### **Options:**

* `--key <KEY>` — node secret key (reads from stdin if not provided)
* `-r`, `--raw-key` — expect a raw key input (32 bytes)
* `--ttl <TTL>` — time to live in seconds (default: unlimited)



## `tycho tool gen-key`

Generate a new key pair

**Usage:** `tycho tool gen-key [OPTIONS]`

###### **Options:**

* `--key <KEY>` — secret key (reads from stdin if only flag is provided)
* `-r`, `--raw-key` — expect a raw key input (32 bytes)



## `tycho tool gen-zerostate`

Generate a zero state for a network

**Usage:** `tycho tool gen-zerostate [OPTIONS] [CONFIG]`

###### **Arguments:**

* `<CONFIG>` — path to the zero state config

###### **Options:**

* `-i`, `--init-config <INIT_CONFIG>` — dump the template of the zero state config
* `-o`, `--output <OUTPUT>` — path to the output file
* `--now <NOW>` — explicit unix timestamp of the zero state
* `-f`, `--force`



## `tycho tool gen-account`

Generate an account state

**Usage:** `tycho tool gen-account <COMMAND>`

###### **Subcommands:**

* `wallet` — Generate a simple wallet state
* `multisig` — Generate a multisig wallet state
* `giver` — Generate a giver state



## `tycho tool gen-account wallet`

Generate a simple wallet state

**Usage:** `tycho tool gen-account wallet --pubkey <PUBKEY> --balance <BALANCE>`

###### **Options:**

* `-p`, `--pubkey <PUBKEY>` — Account public key
* `-b`, `--balance <BALANCE>` — Initial balance of the wallet



## `tycho tool gen-account multisig`

Generate a multisig wallet state

**Usage:** `tycho tool gen-account multisig [OPTIONS] --pubkey <PUBKEY> --balance <BALANCE>`

###### **Options:**

* `-p`, `--pubkey <PUBKEY>` — account public key
* `-b`, `--balance <BALANCE>` — initial balance of the wallet
* `-c`, `--custodians <CUSTODIANS>` — list of custodian public keys
* `-r`, `--req-confirms <REQ_CONFIRMS>` — Number of required confirmations
* `-l`, `--lifetime <LIFETIME>` — Custom lifetime of the wallet
* `-u`, `--updatable` — Use `SetcodeMultisig` instead of `SafeMultisig`



## `tycho tool gen-account giver`

Generate a giver state

**Usage:** `tycho tool gen-account giver --pubkey <PUBKEY> --balance <BALANCE>`

###### **Options:**

* `-p`, `--pubkey <PUBKEY>` — account public key
* `-b`, `--balance <BALANCE>` — initial balance of the giver



## `tycho tool bc`

Blockchain stuff

**Usage:** `tycho tool bc <COMMAND>`

###### **Subcommands:**

* `get-param` — Get blockchain config parameter
* `set-param` — Set blockchain config parameter
* `set-master-key` — Set blockchain config key
* `set-elector-code` — Set elector contract code
* `set-config-code` — Set config contract code



## `tycho tool bc get-param`

Get blockchain config parameter

**Usage:** `tycho tool bc get-param --rpc <RPC> <PARAM>`

###### **Arguments:**

* `<PARAM>` — parameter index

###### **Options:**

* `--rpc <RPC>` — RPC url



## `tycho tool bc set-param`

Set blockchain config parameter

**Usage:** `tycho tool bc set-param [OPTIONS] --rpc <RPC> <PARAM> <VALUE>`

###### **Arguments:**

* `<PARAM>` — parameter index
* `<VALUE>` — parameter value

###### **Options:**

* `--rpc <RPC>` — RPC url
* `--ttl <TTL>` — message ttl

  Default value: `40`
* `--key <KEY>` — secret key (reads from stdin if only flag is provided)
* `-r`, `--raw-key` — expect a raw key input (32 bytes)



## `tycho tool bc set-master-key`

Set blockchain config key

**Usage:** `tycho tool bc set-master-key [OPTIONS] --rpc <RPC> <PUBKEY>`

###### **Arguments:**

* `<PUBKEY>` — new public key

###### **Options:**

* `--rpc <RPC>` — RPC url
* `--ttl <TTL>` — message ttl

  Default value: `40`
* `--key <KEY>` — secret key (reads from stdin if only flag is provided)
* `-r`, `--raw-key` — expect a raw key input (32 bytes)



## `tycho tool bc set-elector-code`

Set elector contract code

**Usage:** `tycho tool bc set-elector-code [OPTIONS] --rpc <RPC> <CODE_PATH>`

###### **Arguments:**

* `<CODE_PATH>` — path to the elector code BOC

###### **Options:**

* `--upgrade-args <UPGRADE_ARGS>` — optional parameters for `after_code_upgrade`
* `--rpc <RPC>` — RPC url
* `--ttl <TTL>` — message ttl

  Default value: `40`
* `--key <KEY>` — secret key (reads from stdin if only flag is provided)
* `-r`, `--raw-key` — expect a raw key input (32 bytes)



## `tycho tool bc set-config-code`

Set config contract code

**Usage:** `tycho tool bc set-config-code [OPTIONS] --rpc <RPC> <CODE_PATH>`

###### **Arguments:**

* `<CODE_PATH>` — path to the config code BOC

###### **Options:**

* `--upgrade-args <UPGRADE_ARGS>` — optional parameters for `after_code_upgrade`
* `--rpc <RPC>` — RPC url
* `--ttl <TTL>` — message ttl

  Default value: `40`
* `--key <KEY>` — secret key (reads from stdin if only flag is provided)
* `-r`, `--raw-key` — expect a raw key input (32 bytes)



## `tycho elect`

Participate in validator elections

**Usage:** `tycho elect <COMMAND>`

###### **Subcommands:**

* `run` — Participate in validator elections
* `once` — Manually participate in validator elections (once)
* `recover` — Recover stake
* `withdraw` — Withdraw funds from the validator wallet
* `get-state` — Get elector contract state



## `tycho elect run`

Participate in validator elections

**Usage:** `tycho elect run [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--rpc <RPC>` — RPC url
* `--force-rpc` — Use rpc even when the control socket file exists
* `-c`, `--config <CONFIG>` — Path to elections config. Default: `$TYCHO_HOME/elections.json`
* `--node-keys <NODE_KEYS>` — Path to node keys. Default: `$TYCHO_HOME/node_keys.json`
* `-s`, `--stake <STAKE>` — Overwrite the stake size
* `--stake-factor <STAKE_FACTOR>` — Max stake factor. Uses config by default
* `--stake-unfreeze-offset <STAKE_UNFREEZE_OFFSET>` — Offset after stake unfreeze time

  Default value: `10m`
* `--elections-start-offset <ELECTIONS_START_OFFSET>` — Time to do nothing after the elections start

  Default value: `10m`
* `--elections-end-offset <ELECTIONS_END_OFFSET>` — Time to stop doing anything before the elections end

  Default value: `2m`
* `--min-retry-interval <MIN_RETRY_INTERVAL>` — Min retry interval in case of error

  Default value: `10s`
* `--max-retry-interval <MAX_RETRY_INTERVAL>` — Max retry interval in case of error

  Default value: `10m`
* `--retry-interval-factor <RETRY_INTERVAL_FACTOR>` — Interval increase factor

  Default value: `2`
* `--disable-random-shift` — Force stakes to be sent right after the elections start



## `tycho elect once`

Manually participate in validator elections (once)

**Usage:** `tycho elect once [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--rpc <RPC>` — RPC url
* `--force-rpc` — Use rpc even when the control socket file exists
* `-c`, `--config <CONFIG>` — Path to elections config. Default: `$TYCHO_HOME/elections.json`
* `--node-keys <NODE_KEYS>` — Path to node keys. Default: `$TYCHO_HOME/node_keys.json`
* `-s`, `--stake <STAKE>` — Overwrite the stake size
* `--stake-factor <STAKE_FACTOR>` — Max stake factor. Uses config by default
* `--wait-balance` — Wait for the account balance to be enough
* `-i`, `--ignore-delivery` — Skip waiting for the message delivery
* `--ttl <TTL>` — Message TTL

  Default value: `40s`



## `tycho elect recover`

Recover stake

**Usage:** `tycho elect recover [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--rpc <RPC>` — RPC url
* `--force-rpc` — Use rpc even when the control socket file exists
* `-c`, `--config <CONFIG>` — Path to elections config. Default: `$TYCHO_HOME/elections.json`
* `--wait-balance` — Wait for the account balance to be enough
* `-i`, `--ignore-delivery` — Skip waiting for the message delivery
* `--ttl <TTL>` — Message TTL

  Default value: `40s`



## `tycho elect withdraw`

Withdraw funds from the validator wallet

**Usage:** `tycho elect withdraw [OPTIONS] --dest <DEST>`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--rpc <RPC>` — RPC url
* `--force-rpc` — Use rpc even when the control socket file exists
* `-c`, `--config <CONFIG>` — Path to elections config. Default: `$TYCHO_HOME/elections.json`
* `-d`, `--dest <DEST>` — Destination address
* `-a`, `--amount <AMOUNT>` — Amount in tokens
* `--all` — Withdraw everything from the wallet
* `--all-but` — Withdraw everything from the wallet reserving at least an `amount` of tokens
* `-b`, `--bounce` — Sets `bounce` message flag
* `-p`, `--payload <PAYLOAD>` — Withdrawal message payload as a base64-encoded BOC
* `--wait-balance` — Wait for the account balance to be enough
* `-i`, `--ignore-delivery` — Skip waiting for the message delivery
* `--ttl <TTL>` — Message TTL

  Default value: `40s`



## `tycho elect get-state`

Get elector contract state

**Usage:** `tycho elect get-state [OPTIONS]`

###### **Options:**

* `--control-socket <CONTROL_SOCKET>` — Path to the control socket. Default: `$TYCHO_HOME/control.sock`
* `--rpc <RPC>` — RPC url
* `--force-rpc` — Use rpc even when the control socket file exists



## `tycho util`

Work with shell environment

**Usage:** `tycho util <COMMAND>`

###### **Subcommands:**

* `markdown-help` — Print a CLI help for all subcommands as Markdown



## `tycho util markdown-help`

Print a CLI help for all subcommands as Markdown

**Usage:** `tycho util markdown-help`



<hr/>

<small><i>
    This document was generated automatically by
    <a href="https://crates.io/crates/clap-markdown"><code>clap-markdown</code></a>.
</i></small>

