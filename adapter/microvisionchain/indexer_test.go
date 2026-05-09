package microvisionchain

import (
	"strings"
	"testing"

	"github.com/btcsuite/btcd/wire"
)

func TestPinIDsFromMsgTxUsesTxHashAndOutputIndex(t *testing.T) {
	tx := wire.NewMsgTx(2)
	tx.AddTxOut(wire.NewTxOut(1, []byte{0x51}))
	tx.AddTxOut(wire.NewTxOut(2, []byte{0x51}))
	txHash := tx.TxHash().String()

	got := pinIDsFromMsgTx(tx)

	if len(got) != 2 {
		t.Fatalf("pinIDsFromMsgTx() len = %d, want 2", len(got))
	}
	if got[0] != txHash+"i0" {
		t.Fatalf("pinIDsFromMsgTx()[0] = %q, want %q", got[0], txHash+"i0")
	}
	if got[1] != txHash+"i1" {
		t.Fatalf("pinIDsFromMsgTx()[1] = %q, want %q", got[1], txHash+"i1")
	}
	for _, id := range got {
		if strings.Contains(id, ":") {
			t.Fatalf("pin id %q unexpectedly contains colon", id)
		}
	}
}
