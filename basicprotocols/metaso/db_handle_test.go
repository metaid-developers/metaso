package metaso

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestTweetCreateMetaIDIndexMatchesQueryField(t *testing.T) {
	name, keys := tweetCreateMetaIDIndex()

	if name != "createmetaid_1" {
		t.Fatalf("index name = %q, want createmetaid_1", name)
	}
	wantKeys := bson.D{{Key: "createmetaid", Value: 1}}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("index keys = %#v, want %#v", keys, wantKeys)
	}
}
