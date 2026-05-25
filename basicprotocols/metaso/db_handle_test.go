package metaso

import (
	"manindexer/database/mongodb"
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

func TestRecommendedFeedIndexesMatchSourceQueries(t *testing.T) {
	got := recommendedFeedIndexes()

	want := []metasoIndexSpec{
		{
			Collection: TweetCollection,
			Name:       "metaid__id_desc",
			Keys:       bson.D{{Key: "metaid", Value: 1}, {Key: "_id", Value: -1}},
		},
		{
			Collection: TweetCollection,
			Name:       "isrecommended__id_desc",
			Keys:       bson.D{{Key: "isrecommended", Value: 1}, {Key: "_id", Value: -1}},
		},
		{
			Collection: TweetCollection,
			Name:       "blocked__id_desc",
			Keys:       bson.D{{Key: "blocked", Value: 1}, {Key: "_id", Value: -1}},
		},
		{
			Collection: TweetCollection,
			Name:       "hot__id_desc_timestamp",
			Keys:       bson.D{{Key: "hot", Value: -1}, {Key: "_id", Value: -1}, {Key: "timestamp", Value: 1}},
		},
		{
			Collection: mongodb.MempoolPinsCollection,
			Name:       "path_metaid__id_desc",
			Keys:       bson.D{{Key: "path", Value: 1}, {Key: "metaid", Value: 1}, {Key: "_id", Value: -1}},
		},
		{
			Collection: mongodb.MempoolPinsCollection,
			Name:       "path_isrecommended__id_desc",
			Keys:       bson.D{{Key: "path", Value: 1}, {Key: "isrecommended", Value: 1}, {Key: "_id", Value: -1}},
		},
		{
			Collection: mongodb.MempoolPinsCollection,
			Name:       "path_blocked__id_desc",
			Keys:       bson.D{{Key: "path", Value: 1}, {Key: "blocked", Value: 1}, {Key: "_id", Value: -1}},
		},
		{
			Collection: mongodb.MempoolPinsCollection,
			Name:       "path_hot__id_desc_timestamp",
			Keys:       bson.D{{Key: "path", Value: 1}, {Key: "hot", Value: -1}, {Key: "_id", Value: -1}, {Key: "timestamp", Value: 1}},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recommendedFeedIndexes() = %#v, want %#v", got, want)
	}
}
