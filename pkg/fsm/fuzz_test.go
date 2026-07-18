package fsm

import "testing"

// FuzzDecodeKVCommand ensures the command decoder never panics on arbitrary
// bytes and that successfully-decoded non-txn commands round-trip stably.
func FuzzDecodeKVCommand(f *testing.F) {
	f.Add(encodeKVCommand(kvCommand{Op: "put", Key: "k", Value: "v"}))
	f.Add(encodeKVCommand(kvCommand{Op: "incr", Key: "c", Value: "-5", ClientID: "cid", SeqNum: 9}))
	f.Add([]byte(`{"op":"put","key":"k","value":"v"}`))
	f.Add([]byte(`{"op":"txn","txn":{}}`))
	f.Add([]byte{})
	f.Add([]byte{0x01})
	f.Add([]byte{0x01, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := decodeKVCommand(data) // must not panic
		if err != nil {
			return
		}
		if cmd.Txn != nil {
			return // JSON txn envelope; binary round-trip not applicable
		}
		re := encodeKVCommand(cmd)
		cmd2, err2 := decodeKVCommand(re)
		if err2 != nil {
			t.Fatalf("re-decode of a valid command failed: %v", err2)
		}
		if cmd2 != cmd {
			t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", cmd2, cmd)
		}
	})
}
