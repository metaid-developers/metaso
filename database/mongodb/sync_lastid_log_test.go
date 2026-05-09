package mongodb

import (
	"testing"
)

func TestSyncLastIdLogIndexSpecUsesUniqueKey(t *testing.T) {
	indexes := syncLastIdLogIndexes()
	if len(indexes) != 1 {
		t.Fatalf("syncLastIdLogIndexes() len = %d, want 1", len(indexes))
	}
	idx := indexes[0]
	if idx.Name != "key_1" {
		t.Fatalf("syncLastIdLog index name = %q, want key_1", idx.Name)
	}
	if !idx.Unique {
		t.Fatal("syncLastIdLog key index must be unique")
	}
	if len(idx.Keys) != 1 || idx.Keys[0].Key != "key" || idx.Keys[0].Value != 1 {
		t.Fatalf("syncLastIdLog index keys = %#v, want key=1", idx.Keys)
	}
}
