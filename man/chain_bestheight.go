package man

import (
	"log"
	"manindexer/adapter"
	"time"
)

// bestHeightRPCTimeout bounds a single GetBestHeight call.
//
// A dead node RPC (e.g. a host that blackholes SYN packets) makes the btcd
// HTTP POST block for minutes, and GetBestHeight runs on paths shared by all
// chains: the startup init, the IndexerRun/CheckNewBlock loop and blockcheck.
// One unreachable chain must never stall the others, so every call is wrapped
// with a hard timeout. On timeout or failure the wrapper returns 0, which all
// callers already treat as "skip this chain this round".
const bestHeightRPCTimeout = 15 * time.Second

func bestHeightWithTimeout(chainName string, chain adapter.Chain, timeout time.Duration) (height int64) {
	if chain == nil {
		return 0
	}
	done := make(chan int64, 1)
	go func() {
		// On timeout this goroutine outlives the call and exits on its own
		// once the RPC client gives up; the buffered channel avoids a leak.
		done <- chain.GetBestHeight()
	}()
	select {
	case height = <-done:
		return height
	case <-time.After(timeout):
		log.Printf("[WARN] GetBestHeight timeout, chain=%s, timeout=%s, skip this chain this round until its RPC recovers", chainName, timeout)
		return 0
	}
}

// BestHeightWithTimeout is the exported form for other packages (blockcheck).
func BestHeightWithTimeout(chainName string) (height int64) {
	return bestHeightWithTimeout(chainName, ChainAdapter[chainName], bestHeightRPCTimeout)
}
