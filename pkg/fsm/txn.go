package fsm

import "encoding/json"

// Compare is a single condition evaluated in a transaction.
// Target selects which field to compare; Result selects the comparison operator.
type Compare struct {
	Key    string `json:"key"`
	Target string `json:"target"` // "value" | "version" | "create_revision" | "mod_revision"
	Result string `json:"result"` // "equal" | "not_equal" | "greater" | "less"
	Value  string `json:"value,omitempty"` // used when Target == "value"
	Rev    int64  `json:"rev,omitempty"`   // used for numeric targets
}

// TxnOp is a single operation within a transaction's success or failure branch.
type TxnOp struct {
	Type  int    `json:"type"`            // 0 = put, 1 = delete
	Key   string `json:"key"`
	Value string `json:"value,omitempty"` // used for put
}

// TxnRequest is the complete transaction payload sent through the Raft log.
type TxnRequest struct {
	Compare []Compare `json:"compare"`
	Success []TxnOp   `json:"success"`
	Failure []TxnOp   `json:"failure"`
}

// TxnResult carries the outcome of a single op within a committed transaction.
type TxnResult struct {
	KV    *KeyValue `json:"kv,omitempty"`
	Error string    `json:"error,omitempty"`
}

// TxnResponse is returned by KVStore.Apply() for a "txn" command and forwarded
// as the HTTP response body by the /v1/txn handler.
type TxnResponse struct {
	Succeeded bool        `json:"succeeded"`
	Results   []TxnResult `json:"results"`
	Revision  int64       `json:"revision"`
}

// txnCommand is the internal JSON envelope used when encoding a transaction for
// the Raft log. It extends the existing kvCommand shape with an optional Txn field.
type txnCommand struct {
	Op  string      `json:"op"`
	Txn *TxnRequest `json:"txn"`
}

// EncodeTxn encodes a TxnRequest as a kvCommand payload ready for raft.Apply().
func EncodeTxn(req *TxnRequest) ([]byte, error) {
	return json.Marshal(txnCommand{Op: "txn", Txn: req})
}

// DecodeTxnResult decodes a TxnResponse from the []byte returned by Apply().
func DecodeTxnResult(data []byte) (*TxnResponse, error) {
	var resp TxnResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// compareInt64 evaluates a numeric comparison.
func compareInt64(actual, expected int64, op string) bool {
	switch op {
	case "equal":
		return actual == expected
	case "not_equal":
		return actual != expected
	case "greater":
		return actual > expected
	case "less":
		return actual < expected
	}
	return false
}
