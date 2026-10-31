package man

import (
	"testing"
	"time"

	"manindexer/pin"
)

type fakeChain struct {
	height     int64
	blockDelay time.Duration
}

func (f *fakeChain) InitChain() {}
func (f *fakeChain) GetBlock(blockHeight int64) (block interface{}, err error) {
	return nil, nil
}
func (f *fakeChain) GetBlockTime(blockHeight int64) (timestamp int64, err error) {
	return 0, nil
}
func (f *fakeChain) GetTransaction(txId string) (tx interface{}, err error) {
	return nil, nil
}
func (f *fakeChain) GetInitialHeight() (height int64) { return 0 }
func (f *fakeChain) GetBestHeight() (height int64) {
	if f.blockDelay > 0 {
		time.Sleep(f.blockDelay)
	}
	return f.height
}
func (f *fakeChain) GetBlockMsg(height int64) (blockMsg *pin.BlockMsg) { return nil }
func (f *fakeChain) GetMempoolTransactionList() (list []interface{}, err error) {
	return nil, nil
}
func (f *fakeChain) GetTxSizeAndFees(txHash string) (fee int64, size int64, blockHash string, err error) {
	return 0, 0, "", nil
}

func TestBestHeightWithTimeoutReturnsHealthyHeight(t *testing.T) {
	chain := &fakeChain{height: 12345}

	got := bestHeightWithTimeout("test", chain, 2*time.Second)

	if got != 12345 {
		t.Fatalf("bestHeightWithTimeout() = %d, want 12345", got)
	}
}

func TestBestHeightWithTimeoutSkipsHangingChain(t *testing.T) {
	// Simulates a dead node RPC whose underlying HTTP POST never returns.
	chain := &fakeChain{height: 99999, blockDelay: 10 * time.Second}

	start := time.Now()
	got := bestHeightWithTimeout("test", chain, 100*time.Millisecond)
	elapsed := time.Since(start)

	if got != 0 {
		t.Fatalf("bestHeightWithTimeout() = %d, want 0 for hanging chain", got)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("bestHeightWithTimeout() took %s, must return near the 100ms timeout", elapsed)
	}
}

func TestBestHeightWithTimeoutHandlesNilChain(t *testing.T) {
	got := bestHeightWithTimeout("test", nil, 100*time.Millisecond)

	if got != 0 {
		t.Fatalf("bestHeightWithTimeout() = %d, want 0 for nil chain", got)
	}
}
