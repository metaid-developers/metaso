package man

import "testing"

func TestClampMempoolCleanupHeightCapsHistoricalCatchup(t *testing.T) {
	bestHeight := int64(1000)
	current := int64(200)

	got := clampMempoolCleanupHeight(current, bestHeight, 100)

	if got != 900 {
		t.Fatalf("clampMempoolCleanupHeight() = %d, want 900", got)
	}
}

func TestClampMempoolCleanupHeightKeepsRecentHeight(t *testing.T) {
	bestHeight := int64(1000)
	current := int64(950)

	got := clampMempoolCleanupHeight(current, bestHeight, 100)

	if got != 950 {
		t.Fatalf("clampMempoolCleanupHeight() = %d, want 950", got)
	}
}

func TestClampMempoolCleanupHeightNeverGoesBelowZero(t *testing.T) {
	bestHeight := int64(50)
	current := int64(0)

	got := clampMempoolCleanupHeight(current, bestHeight, 100)

	if got != 0 {
		t.Fatalf("clampMempoolCleanupHeight() = %d, want 0", got)
	}
}
