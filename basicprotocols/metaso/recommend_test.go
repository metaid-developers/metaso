package metaso

import (
	"manindexer/database/mongodb"
	"reflect"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestBuildRecommendedSourcePipelineLimitsBranchesBeforeUnion(t *testing.T) {
	filter := bson.D{
		{Key: "isrecommended", Value: true},
		{Key: "id", Value: bson.D{{Key: "$nin", Value: []string{"seen-a", "seen-b"}}}},
	}
	sort := bson.D{{Key: "_id", Value: -1}}

	got := buildRecommendedSourcePipeline(filter, sort, 2)

	want := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$sort", Value: sort}},
		{{Key: "$limit", Value: int64(2)}},
		{{Key: "$unionWith", Value: bson.D{
			{Key: "coll", Value: mongodb.MempoolPinsCollection},
			{Key: "pipeline", Value: mongo.Pipeline{
				{{Key: "$match", Value: append(DataFilter, filter...)}},
				{{Key: "$sort", Value: sort}},
				{{Key: "$limit", Value: int64(2)}},
			}},
		}}},
		{{Key: "$sort", Value: sort}},
		{{Key: "$limit", Value: int64(2)}},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildRecommendedSourcePipeline() = %#v, want %#v", got, want)
	}
}

func TestRecentReadedPinIDsKeepsLatestUniquePins(t *testing.T) {
	readedLog := []byte("old_1,dup_2,other_3,dup_4,new_5,")

	got := recentReadedPinIDs(readedLog, 3)

	want := []string{"new", "dup", "other"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recentReadedPinIDs() pins = %#v, want %#v", got, want)
	}
}

func TestRecentReadedPinIDsStopsAfterLatestUniqueLimit(t *testing.T) {
	readedLog := []byte("old_1,older_2,newer_3,newest_4,")

	got := recentReadedPinIDs(readedLog, 2)

	want := []string{"newest", "newer"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recentReadedPinIDs() pins = %#v, want %#v", got, want)
	}
}

func TestFormatReadedPinsForMergeWritesOldestToNewest(t *testing.T) {
	pinIDsNewestFirst := []string{"new", "middle", "old"}

	got := formatReadedPinsForMerge(pinIDsNewestFirst, 1779672161)

	want := "old_1779672161,middle_1779672161,new_1779672161,"
	if got != want {
		t.Fatalf("formatReadedPinsForMerge() = %q, want %q", got, want)
	}
	if strings.Contains(got, "new_1779672161,middle") {
		t.Fatalf("formatReadedPinsForMerge() wrote newest first: %q", got)
	}
}
