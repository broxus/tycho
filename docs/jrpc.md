JSON-RPC (JRPC) light server protocol for the Tycho node.

## Methods

- [`getCapabilities`](#getcapabilities)
- [`getLatestKeyBlock`](#getlatestkeyblock)
- [`getBlockchainConfig`](#getblockchainconfig)
- [`getStatus`](#getstatus)
- [`getTimings`](#gettimings)
- [`getContractState`](#getcontractstate)
- [`getLibraryCell`](#getlibrarycell)
- [`sendMessage`](#sendmessage)
- [`getTransactionsList`](#gettransactionslist)
- [`getTransaction`](#gettransaction)
- [`getDstTransaction`](#getdsttransaction)
- [`getAccountsByCodeHash`](#getaccountsbycodehash)
- [`getKeyBlockProof`](#getKeyBlockProof)
- [`getTransactionBlockId`](#gettransactionblockid)

### `getCapabilities`

Returns a list of supported methods of the JRPC

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getCapabilities",
  "params": {}
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": string[], // List of supported methods
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getCapabilities",
    "params": {}
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": [
        "getCapabilities",
        "getLatestKeyBlock",
        "getBlockchainConfig",
        "getStatus",
        "getTimings",
        "getContractState",
        "sendMessage",
        "getLibraryCell",
        "getKeyBlockProof",
        "getTransactionsList",
        "getTransaction",
        "getDstTransaction",
        "getAccountsByCodeHash",
        "getTransactionBlockId"
    ]
  }
  ```
</details>

---

### `getLatestKeyBlock`

Retrieves the latest key block from the blockchain.

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getLatestKeyBlock",
  "params": {}
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "block": string // Base64 encoded key block
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getLatestKeyBlock",
    "params": {}
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "block":"te6ccgICBN4AAQAAs...V5kdk/9OjYskr9go="
    }
  }
  ```
</details>

---

### `getBlockchainConfig`

Retrieves the latest blockchain config from the blockchain.

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getBlockchainConfig",
  "params": {}
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "globalId": number, // Network id
    "seqno": number, // Block number
    "config": string // Base64 encoded config params
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getBlockchainConfig",
    "params": {}
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
      "globalId": 2000,
      "seqno": 4354001,
      "config": "te6ccgICAQQAAQAAH6w...VVVVVVVVVVVV"
    }
  }
  ```
</details>

---

### `getStatus`

Returns a node JRPC module status.

> Not applicable to public endpoint

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getStatus",
  "params": {}
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "ready": boolean, // Whether the JRPC module is ready
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getStatus",
    "params": {}
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
      "ready": true
    }
  }
  ```
</details>

---

### `getTimings`

Returns node state timings.

> Not applicable to public endpoint

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getTimings",
  "params": {}
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "lastMcBlockSeqno": number, // Number of the latest known masterchain block
    "lastMcUtime": number, // Unix timestamp of the latest known masterchain block
    "mcTimeDiff": number, // Lag of the latest known masterchain block
    "smallestKnownLt": number, // Smallest logical time
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTimings",
    "params": {}
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
      "lastMcBlockSeqno": 4360491,
      "lastMcUtime": 1753445129,
      "mcTimeDiff": 1,
      "smallestKnownLt": 9861665000000
    }
  }
  ```
</details>

---

### `getLibraryCell`

Retrieves library cell by its representation hash.

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getLibraryCell",
  "params": {
    "hash": string // hex encoded 32 bytes of cell representation hash
  }
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "cell": string, // Base64 encoded boc
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getLibraryCell",
    "params": {
      "hash": "a4e12e97e67169c73c199b74e96290d7387d596a45089ca8424f3f918fff55c2"
    }
  }'
  ```

  **Response:**
  ```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "cell": "te6ccgECyAEAEtEA...BDELxqTePeA="
  }
}
  ```
</details>

---

### `getContractState`

Retrieves the state of an account at the specific address.

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getContractState",
  "params": {
    "address": string // Contract address
  }
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "type": "notExists"
  } | {
    "type": "exists",
    "account": string, // Base64 encoded account state (AccountStuff model)
    "lastTransactionId": {
      "isExact": boolean,
      "hash": string, // Hex encoded transaction hash
      "lt": string, // Transaction logical time (u64)
    },
    "timings": {
      "genLt": string, // Logical time of the shard state for the requested account
      "genUtime": number, // Unix timestamp of the shard state for the requested account
    }
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getContractState",
    "params": {
      "address": "-1:3333333333333333333333333333333333333333333333333333333333333333"
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
      "type": "exists",
      "account": "te6ccgICA94AAQAA...9AT6ANMf0//R",
      "timings": {
        "genLt": "10072981000004",
        "genUtime": 1753445568
      },
      "lastTransactionId": {
        "isExact": true,
        "lt": "10072981000002",
        "hash": "d38aebb4b04a815309ea062bcbe817904fc3da4627e8abfcc779e49762ffcd12"
      }
    }
  }
  ```
</details>

---

### `getAccountsByCodeHash`

Retrieves a sorted list of addresses for contracts with the specified code hash.

Use the last address from the list for the `continuation`.

> Requres the full API mode

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getAccountsByCodeHash",
  "params": {
    "codeHash": string, // Hex encoded code hash
    "limit": number, // Max number of items in response (at most 100)
    "continuation": string | undefined, // Optional address as a continuation (>)
  },
  "id": 1
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": string[], // List of addresses
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getAccountsByCodeHash",
    "params": {
      "codeHash": "d66d198766abdbe1253f3415826c946c371f5112552408625aeb0b31e0ef2df3",
      "limit": 5 
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": [
        "0:06a2c6a56026000bd00d692f039b400eb30f00883b7d3c3c3a14e1ef1e1ac996",
        "0:06b32befc13dffbeb14a83655a79c3d050abbac184dfd7930c1b697574d57d3c",
        "0:07711d7523f094f35316863c64a16f048c2ff776d4c456acd9f156ed0465e471",
        "0:0a8e63e8967b0ef23a5f4738076c98ca129ecd94fbd9dc5deab3ee6781d3abb6",
        "0:0db5e698eb19ef235b02c78146ab7321e90f1e412e5674ff4cf65fea26f3cda3"
    ]
  }
  ```
</details>

---

### `getTransactionsList`

Retrieves a list of raw transactions in the descending order.

Use the `prevTransactionLt` the last transaction in the list for the `continuation`.

> Requres the full API mode

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getTransactionsList",
  "params": {
    "account": string, // Account address
    "limit": number, // Max number of items in response (at most 100)
    "lastTransactionLt": string | undefined, // Optional logical time to start from (>=)
  }
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": string[], // List of base64 encoded transactions
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTransactionsList",
    "params": {
      "account": "-1:3333333333333333333333333333333333333333333333333333333333333333",
      "limit": 5
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": [
      "te6ccgECBwEAAYk...AMk4nHRA",
      "te6ccgECBgEAASw...ybpEAAEg",
      "te6ccgECBwEAAYk...gMk4nG5A",
      "te6ccgECBgEAASw...omdqAAEg",
      "te6ccgECBgEAASw...SZ2RAAEg"
    ]
  }
  ```
</details>

---

### `getTransaction`

Searches for a transaction by the id.

> Requres the full API mode

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getTransaction",
  "params": {
    "id": string, // Hex encoded transaction hash
  }
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": string | undefined, // Optional base64 encoded transaction
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTransaction",
    "params": {
      "id": "26380563b9df3833d73574a93e9c5f833118561d821d08da44fa2d8549c90cbc"
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": "te6ccgECBw...t1AMk4nk5A"
  }
  ```
</details>

---

### `getDstTransaction`

Searches for a transaction by the id of an incoming message.

> Requres the full API mode

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getDstTransaction",
  "params": {
    "messageHash": string, // Hex encoded message hash
  }
}
```

**Response:**
```typescript
{
  "jsonrpc":"2.0",
  "id": number, // Request id
  "result": string | undefined, // Optional base64 encoded transaction
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getDstTransaction",
    "params": {
      "messageHash": "a77e87d6e0f0b3867de9ce3bf711ce9bff0a17d04ed14e36f9912c4fa8afdace"
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": "te6ccgECBwEAAY...AMk4nk5A"
  }
  ```
</details>

---

### `getKeyBlockProof`

Retrieves the proof of an key block at the specific key block number.

> Requres the full API mode

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getKeyBlockProof",
  "params": {
    "seqno": string, // Key block number
  }
}
```

**Response:**
```typescript
{
    "jsonrpc": "2.0",
    "id": number, // Request id
    "result": {
        "blockId": string , // Full block identification includes: ID, shard, sequence number, block hash, and file hash.
        "proof": string // Block proof data
    }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getKeyBlockProof",
    "params": {
      "seqno": 4354001
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "blockId": "-1:8000000000000000:4354001:a7f35fe1637f6311c7cf580f09ef90c5a9aa6f57183b7ec969973cc93787570d:1b4d30cbb6d4b44266fd05eeb9b5ba0cfeb3f7aafc30932f3bb65bc9a58562b5",
        "proof": "te6ccgEChAEAE14A...cUxDWf83Iig="
    }
  }
  ```
</details>

---


### `getTransactionBlockId`

Searches for a block identifier by transaction id.

> Requres the full API mode

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getTransactionBlockId",
  "params": {
    "id": string, // Hex encoded transaction hash
  }
}
```

**Response:**
```typescript
{
    "jsonrpc": "2.0",
    "id": number, // Request id
    "result": {
        "blockId": string // Full block identification includes: ID, shard, sequence number, block hash, and file hash.
    }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTransactionBlockId",
    "params": {
      "id": "0c50cf950b8b8a15f894ff995ec2fea02b3d91a05fe4a3025dfceb643b9ccb86"
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "blockId": "0:8000000000000000:5712486:bc4b693739409e4f876ecad2eba1615d91927905bf432e1891b2ec454f8e7a00:f8fb84052e2d026724af9972af6ac97cdc638231ec70f7ea5da46e8454676c0f"
    }
  }
  ```
</details>

---

### `sendMessage`

Broadcasts an external message.

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "sendMessage",
  "params": {
    "message": string, // Base64 encoded message
  }
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": null,
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://rpc-testnet.tychoprotocol.com/proto -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "sendMessage",
    "params": {
      "message": "te6ccgEBAQEAdQAA5Yg...+nNzEuRY="
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": null
  }
  ```
</details>

---