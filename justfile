# Define default recipe
default: fmt lint docs test

install_fmt:
    rustup component add rustfmt --toolchain nightly

# helpers
fmt: install_fmt
    cargo +nightly fmt --all

# ci checks
check_format: install_fmt
    cargo +nightly fmt --all -- --check

lint: check_format
    cargo clippy --all-targets --all-features --workspace

docs: check_format
    export RUSTDOCFLAGS=-D warnings
    cargo doc --no-deps --document-private-items --all-features --workspace

test: lint
    cargo test --all-targets --all-features --workspace

gen_network n:
    #!/usr/bin/env bash
    cargo build --bin tycho
    TEMP_DIR="./.temp"
    TYCHO_BIN="./target/debug/tycho"

    N={{n}}

    GLOBAL_CONFIG='{}'
    NODE_CONFIG=$(cat ./config.json)

    for i in $(seq $N);
    do
        $TYCHO_BIN tool gen-key > "$TEMP_DIR/keys${i}.json"

        PORT=$((20000 + i))

        KEY=$(jq -r .secret < "$TEMP_DIR/keys${i}.json")
        DHT_ENTRY=$($TYCHO_BIN tool gen-dht "127.0.0.1:$PORT" --key "$KEY")

        GLOBAL_CONFIG=$(echo "$GLOBAL_CONFIG" | jq ".bootstrap_peers += [$DHT_ENTRY]")

        NODE_CONFIG=$(echo "$NODE_CONFIG" | jq ".port = $PORT | .storage.root_dir = \"$TEMP_DIR/db${i}\"")
        echo "$NODE_CONFIG" > "$TEMP_DIR/config${i}.json"
    done

    ZEROSTATE=$(cat zerostate.json | jq '.validators = []')
    for i in $(seq $N);
    do
        PUBKEY=$(jq .public < "$TEMP_DIR/keys${i}.json")
        ZEROSTATE=$(echo "$ZEROSTATE" | jq ".validators += [$PUBKEY]")
    done

    echo "$ZEROSTATE" > "$TEMP_DIR/zerostate.json"
    ZEROSTATE_ID=$(
        $TYCHO_BIN tool gen-zerostate "$TEMP_DIR/zerostate.json" \
            --output "$TEMP_DIR/zerostate.boc" \
            --force
    )

    GLOBAL_CONFIG=$(echo "$GLOBAL_CONFIG" | jq ".zerostate = $ZEROSTATE_ID")
    echo "$GLOBAL_CONFIG" > "$TEMP_DIR/global-config.json"

node n:
    #!/usr/bin/env bash
    cargo build --bin tycho
    TEMP_DIR="./.temp"
    TYCHO_BIN="./target/debug/tycho"

    $TYCHO_BIN node run \
        --keys "$TEMP_DIR/keys{{n}}.json" \
        --config "$TEMP_DIR/config{{n}}.json" \
        --global-config "$TEMP_DIR/global-config.json" \
        --import-zerostate "$TEMP_DIR/zerostate.boc" \
        --logger-config ./logger.json \

init_node_config:
    #!/usr/bin/env bash
    cargo run --bin tycho -- --init-config "./config.json"
