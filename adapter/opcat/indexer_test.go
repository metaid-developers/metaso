package opcat

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"manindexer/common"
	"manindexer/pin"
)

const knownOpcatPinScriptHex = "6a4c98066d657461696406637265617465152f70726f746f636f6c732f73696d706c6562757a7a013005312e302e30106170706c69636174696f6e2f6a736f6e4c597b22636f6e74656e74223a2268656c6c6f204f50434154222c22636f6e74656e7454797065223a22746578742f706c61696e3b7574662d38222c226174746163686d656e7473223a5b5d2c2271756f746550696e223a22227d"

func setupOpcatTestConfig() {
	common.Config = &common.AllConfig{
		ProtocolID:  "6d6574616964",
		SyncHost:    []string{"*"},
		BlockedHost: []string{},
	}
	common.Config.Opcat.PopCutNum = 21
}

func TestParsePinParsesKnownOpcatNestedOpReturn(t *testing.T) {
	setupOpcatTestConfig()

	pinScript, err := hex.DecodeString(knownOpcatPinScriptHex)
	if err != nil {
		t.Fatalf("decode PIN script: %v", err)
	}

	indexer := &Indexer{ChainName: "opcat"}
	indexer.InitIndexer()
	parsed := indexer.ParsePin(pinScript)
	if parsed == nil {
		t.Fatal("expected known OPCAT PIN script to parse")
	}
	if parsed.Path != "/protocols/simplebuzz" {
		t.Fatalf("unexpected path: %s", parsed.Path)
	}
	if string(parsed.ContentBody) != `{"content":"hello OPCAT","contentType":"text/plain;utf-8","attachments":[],"quotePin":""}` {
		t.Fatalf("unexpected content body: %s", string(parsed.ContentBody))
	}
}

func TestGetBase58AddressFromPkScriptInvalidHashReturnsEmpty(t *testing.T) {
	indexer := &Indexer{}
	indexer.InitIndexer()

	for _, scriptAddress := range [][]byte{
		nil,
		{0x01, 0x02, 0x03},
	} {
		if got := GetBase58AddressFromPkScript(scriptAddress, netParams); got != "" {
			t.Fatalf("expected empty address for invalid script address %x, got %q", scriptAddress, got)
		}
	}
}

func TestCatchPinsByVerboseTxStoresOpcatChainName(t *testing.T) {
	setupOpcatTestConfig()

	indexer := &Indexer{ChainName: "opcat"}
	indexer.InitIndexer()

	tx := opcatVerboseTx{
		TxID: "752cd90927691885ed237b4936d7a1f69799fe4e40cb2df6e8773a2c1bc870eb",
		Vout: []opcatVerboseVout{
			{N: 0, Value: json.Number("0")},
			{N: 1, Value: json.Number("0.00003997")},
		},
	}
	tx.Vout[0].ScriptPubKey.Hex = knownOpcatPinScriptHex
	tx.Vout[1].ScriptPubKey.Hex = "76a9149449c2b36e3580d00c6b89406f9f9a2b20fcc25788ac"

	pins := indexer.catchPinsByVerboseTx(tx, 121347, 1778257307, "", "", 0)
	if len(pins) != 1 {
		t.Fatalf("expected one PIN, got %d", len(pins))
	}
	if pins[0].ChainName != "opcat" {
		t.Fatalf("unexpected chain name: %s", pins[0].ChainName)
	}
	if pins[0].Id != "752cd90927691885ed237b4936d7a1f69799fe4e40cb2df6e8773a2c1bc870ebi0" {
		t.Fatalf("unexpected pin id: %s", pins[0].Id)
	}
	if pins[0].Path != "/protocols/simplebuzz" {
		t.Fatalf("unexpected path: %s", pins[0].Path)
	}
	if pins[0].Address != "1EX5NN6npyCp3X6Sv4Yahv6DrBNKRtq4Gw" {
		t.Fatalf("unexpected owner address: %s", pins[0].Address)
	}
	if !strings.Contains(string(pins[0].ContentBody), "hello OPCAT") {
		t.Fatalf("unexpected content body: %s", string(pins[0].ContentBody))
	}
}

func TestCatchTransferIgnoresVerboseBlockWithoutTransactions(t *testing.T) {
	indexer := &Indexer{ChainName: "opcat", Block: opcatVerboseBlock{}}
	indexer.InitIndexer()

	transfers := indexer.CatchTransfer(map[string]string{
		"previous-tx:0": "1JQheacLPdM5ySCkrZkV66G2ApAXe1mqLj",
	})
	if len(transfers) != 0 {
		t.Fatalf("expected no transfers for empty verbose block, got %d", len(transfers))
	}
}

func TestCatchTransferByVerboseTxBuildsTransferInfo(t *testing.T) {
	indexer := &Indexer{ChainName: "opcat"}
	indexer.InitIndexer()

	tx := opcatVerboseTx{
		TxID: "new-tx",
		Vin: []struct {
			TxID string `json:"txid"`
			Vout uint32 `json:"vout"`
		}{
			{TxID: "previous-tx", Vout: 0},
		},
		Vout: []opcatVerboseVout{
			{N: 0, Value: json.Number("0.00003997")},
		},
	}
	tx.Vout[0].ScriptPubKey.Hex = "76a9149449c2b36e3580d00c6b89406f9f9a2b20fcc25788ac"

	transfers := make(map[string]*pin.PinTransferInfo)
	indexer.catchTransferByVerboseTx(tx, map[string]string{
		"previous-tx:0": "from-address",
	}, transfers)

	info, ok := transfers["previous-tx:0"]
	if !ok {
		t.Fatal("expected transfer info for previous output")
	}
	if info.FromAddress != "from-address" {
		t.Fatalf("unexpected from address: %s", info.FromAddress)
	}
	if info.Address != "1EX5NN6npyCp3X6Sv4Yahv6DrBNKRtq4Gw" {
		t.Fatalf("unexpected to address: %s", info.Address)
	}
	if info.Output != "new-tx:0" {
		t.Fatalf("unexpected output: %s", info.Output)
	}
	if info.Location != "new-tx:0:0" {
		t.Fatalf("unexpected location: %s", info.Location)
	}
	if info.OutputValue != 3997 {
		t.Fatalf("unexpected output value: %d", info.OutputValue)
	}
}

func TestPinIDsFromVerboseTxUsesVerboseOutputIndexes(t *testing.T) {
	tx := opcatVerboseTx{
		TxID: "abc",
		Vout: []opcatVerboseVout{
			{N: 0},
			{N: 2},
		},
	}
	got := pinIDsFromVerboseTx(tx)
	want := []string{"abci0", "abci2"}
	if len(got) != len(want) {
		t.Fatalf("expected %d ids, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("id %d = %q, want %q", i, got[i], want[i])
		}
	}
}
