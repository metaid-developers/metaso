package metaso

import (
	"manindexer/database/mongodb"
	"reflect"
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
