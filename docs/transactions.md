# Transactions

The KV store supports compare-and-swap (CAS) transactions modelled after etcd's transaction API. A transaction evaluates a set of conditions against the current FSM state and, depending on whether all conditions pass, executes one of two operation branches atomically.

## Transaction model

```
if all(Compare) → execute Success ops
else             → execute Failure ops
```

The entire transaction — comparisons, success or failure branch — is encoded as a single Raft log entry and applied atomically to the FSM. No concurrent write can interleave. Either all operations in the winning branch succeed, or none of them does (pre-validation aborts on the first failing op).

A transaction always increments the global revision by exactly one, regardless of how many operations it contains.

---

## Request format

`POST /v1/txn` with a JSON body:

```json
{
  "compare": [
    {
      "key": "lock",
      "target": "value",
      "result": "equal",
      "value": ""
    }
  ],
  "success": [
    {"type": 0, "key": "lock", "value": "acquired"},
    {"type": 0, "key": "data", "value": "critical-section-value"}
  ],
  "failure": []
}
```

### `compare` array

Each element is a `Compare` condition:

| Field | Type | Description |
|---|---|---|
| `key` | string | The key to inspect |
| `target` | string | Which field to compare: `"value"`, `"version"`, `"create_revision"`, or `"mod_revision"` |
| `result` | string | Comparison operator: `"equal"`, `"not_equal"`, `"greater"`, or `"less"` |
| `value` | string | Value to compare against when `target == "value"` |
| `rev` | int64 | Revision/version to compare against for numeric targets |

A missing key has `version=0`, `create_revision=0`, `mod_revision=0`, and a `target=="value"` with `result=="not_equal"` evaluates to `true` for any non-empty comparand.

### `success` and `failure` arrays

Each element is a `TxnOp`:

| Field | Type | Description |
|---|---|---|
| `type` | int | `0` = put, `1` = delete |
| `key` | string | The key to operate on |
| `value` | string | Value to write (put only) |

---

## Response format

```json
{
  "succeeded": true,
  "results": [
    {"kv": {"key":"lock","value":"acquired","create_revision":5,"mod_revision":5,"version":1,"expires_at_ms":0}},
    {"kv": {"key":"data","value":"critical-section-value","create_revision":5,"mod_revision":5,"version":1,"expires_at_ms":0}}
  ],
  "revision": 5
}
```

| Field | Description |
|---|---|
| `succeeded` | `true` if all `compare` conditions passed and the `success` branch ran |
| `results` | Per-operation results from the branch that executed |
| `revision` | The global revision after the transaction was applied |

---

## Examples

### Compare-and-swap (CAS) lock

Acquire a lock only if the `lock` key does not exist yet:

```bash
curl -s -X POST \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d '{
    "compare": [{"key":"lock","target":"version","result":"equal","rev":0}],
    "success": [{"type":0,"key":"lock","value":"node1"}],
    "failure": []
  }' \
  http://localhost:8012/v1/txn | jq .
```

Response when successful (`version` was 0, meaning key absent):

```json
{"succeeded": true, "results": [{"kv": {"key":"lock","value":"node1",...}}], "revision": 1}
```

Response when the lock is already held:

```json
{"succeeded": false, "results": [], "revision": 1}
```

### Conditional update

Update `counter` only if it currently equals `"5"`, and delete `temp` at the same time:

```bash
curl -s -X POST \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d '{
    "compare": [{"key":"counter","target":"value","result":"equal","value":"5"}],
    "success": [
      {"type":0,"key":"counter","value":"6"},
      {"type":1,"key":"temp"}
    ],
    "failure": [{"type":0,"key":"counter_conflict","value":"seen"}]
  }' \
  http://localhost:8012/v1/txn | jq .
```

### Check-and-set using revision

Only update a key if no other writer has changed it since you last read it (optimistic concurrency using `mod_revision`):

```bash
# First, read the key to get its mod_revision
REV=$(curl -s -H "Authorization: Bearer secret" http://localhost:8012/v1/kv/mykey | jq .mod_revision)

# Then submit a conditional update
curl -s -X POST \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d "{
    \"compare\": [{\"key\":\"mykey\",\"target\":\"mod_revision\",\"result\":\"equal\",\"rev\":$REV}],
    \"success\": [{\"type\":0,\"key\":\"mykey\",\"value\":\"new-value\"}],
    \"failure\": []
  }" \
  http://localhost:8012/v1/txn | jq .
```

---

## kvctl transaction

```bash
kvctl --leader=localhost:8012 txn \
  --compare 'key=lock,target=version,result=equal,rev=0' \
  --success  'put,key=lock,value=node1' \
  --failure  ''
```

See `kvctl txn --help` for full syntax.

---

## Atomicity guarantees

- All operations in a branch succeed or none do. A pre-validation pass rejects the transaction before any state is mutated if any operation would fail (e.g., deleting a missing key).
- The transaction is a single Raft log entry. It is committed and applied exactly once.
- Watch events for all operations in a transaction share the same revision (the single revision increment for that transaction).
- Idempotency: include a `client_id` and `seq_num` in the outer command to deduplicate retries after network failures.
