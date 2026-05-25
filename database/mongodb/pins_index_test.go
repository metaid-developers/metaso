package mongodb

import "testing"

func TestPinLookupIndexesSupportCreateAddressBackfill(t *testing.T) {
	indexes := pinLookupIndexes()
	if len(indexes) != 1 {
		t.Fatalf("pinLookupIndexes() len = %d, want 1", len(indexes))
	}
	idx := indexes[0]
	if idx.Name != "createaddress__id_asc" {
		t.Fatalf("pin lookup index name = %q, want createaddress__id_asc", idx.Name)
	}
	if idx.Unique {
		t.Fatal("pin lookup index must not be unique")
	}
	if len(idx.Keys) != 2 ||
		idx.Keys[0].Key != "createaddress" || idx.Keys[0].Value != 1 ||
		idx.Keys[1].Key != "_id" || idx.Keys[1].Value != 1 {
		t.Fatalf("pin lookup index keys = %#v, want createaddress=1,_id=1", idx.Keys)
	}
}
