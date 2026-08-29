package metaso

import (
	"manindexer/database/mongodb"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestBuildCommentsListKeepsCommentsMissingBuzzViewMetrics(t *testing.T) {
	comments := []*TweetComment{
		{
			PinId:         "missing-from-buzzview",
			ChainName:     "mvc",
			CommentPinId:  "parent",
			Content:       "on-chain comment",
			CreateAddress: "address-a",
			CreateMetaid:  "metaid-a",
			Timestamp:     1779179374,
		},
		{
			PinId:         "with-metrics",
			ChainName:     "mvc",
			CommentPinId:  "parent",
			Content:       "comment with metrics",
			CreateAddress: "address-b",
			CreateMetaid:  "metaid-b",
			Timestamp:     1779181064,
		},
	}
	metrics := []*Tweet{
		{
			Id:           "with-metrics",
			LikeCount:    3,
			CommentCount: 2,
		},
	}

	got := buildCommentsList(comments, metrics)

	if len(got) != 2 {
		t.Fatalf("buildCommentsList() len = %d, want 2", len(got))
	}
	if got[0].PinId != "missing-from-buzzview" {
		t.Fatalf("first comment pinId = %q, want missing-from-buzzview", got[0].PinId)
	}
	if got[0].LikeNum != 0 || got[0].CommentNum != 0 {
		t.Fatalf("missing metrics counts = (%d,%d), want (0,0)", got[0].LikeNum, got[0].CommentNum)
	}
	if got[1].PinId != "with-metrics" {
		t.Fatalf("second comment pinId = %q, want with-metrics", got[1].PinId)
	}
	if got[1].LikeNum != 3 || got[1].CommentNum != 2 {
		t.Fatalf("metric counts = (%d,%d), want (3,2)", got[1].LikeNum, got[1].CommentNum)
	}
}

func TestAppendCommentIfMissingSkipsDuplicatePinId(t *testing.T) {
	comments := []*CommentsList{
		{
			PinId:     "same-comment-pin",
			Content:   "confirmed comment",
			Timestamp: 1779191190,
		},
	}
	mempoolComment := &CommentsList{
		PinId:     "same-comment-pin",
		Content:   "mempool comment",
		Timestamp: 1779190972,
	}

	appended := appendCommentIfMissing(&comments, mempoolComment)

	if appended {
		t.Fatal("appendCommentIfMissing() appended duplicate pinId, want skip")
	}
	if len(comments) != 1 {
		t.Fatalf("comments len = %d, want 1", len(comments))
	}
	if comments[0].Timestamp != 1779191190 {
		t.Fatalf("kept timestamp = %d, want confirmed timestamp 1779191190", comments[0].Timestamp)
	}
}

func TestAppendCommentIfMissingAppendsNewPinId(t *testing.T) {
	comments := []*CommentsList{
		{PinId: "confirmed-comment"},
	}
	mempoolComment := &CommentsList{
		PinId:     "new-mempool-comment",
		Content:   "new comment",
		Timestamp: 1779190972,
	}

	appended := appendCommentIfMissing(&comments, mempoolComment)

	if !appended {
		t.Fatal("appendCommentIfMissing() skipped new pinId, want append")
	}
	if len(comments) != 2 {
		t.Fatalf("comments len = %d, want 2", len(comments))
	}
	if comments[1].PinId != "new-mempool-comment" {
		t.Fatalf("appended pinId = %q, want new-mempool-comment", comments[1].PinId)
	}
}

func TestPrepareTweetFeedItemsDedupesByPinIdAndKeepsFirst(t *testing.T) {
	tweets := []*Tweet{
		{
			Id:          "same-buzz",
			ContentBody: []byte("first"),
		},
		{
			Id:          "same-buzz",
			ContentBody: []byte("duplicate"),
		},
		{
			Id:          "other-buzz",
			ContentBody: []byte("other"),
		},
	}

	got, pinIds := prepareTweetFeedItems(tweets)

	if len(got) != 2 {
		t.Fatalf("deduped list len = %d, want 2", len(got))
	}
	if got[0].Id != "same-buzz" || got[0].Content != "first" {
		t.Fatalf("first item = (%q,%q), want same-buzz first", got[0].Id, got[0].Content)
	}
	if len(got[0].ContentBody) != 0 {
		t.Fatalf("first ContentBody len = %d, want 0", len(got[0].ContentBody))
	}
	if got[1].Id != "other-buzz" {
		t.Fatalf("second item id = %q, want other-buzz", got[1].Id)
	}
	if len(pinIds) != 2 || pinIds[0] != "same-buzz" || pinIds[1] != "other-buzz" {
		t.Fatalf("pinIds = %#v, want [same-buzz other-buzz]", pinIds)
	}
}

func TestBuildNewestFeedPipelineLimitsBranchesBeforeUnion(t *testing.T) {
	cursor := primitive.NewObjectID()
	filter := bson.D{
		{Key: "blocked", Value: false},
		{Key: "_id", Value: bson.D{{Key: "$lt", Value: cursor}}},
	}

	got := buildNewestFeedPipeline(filter, 10)

	want := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: -1}}}},
		{{Key: "$limit", Value: int64(10)}},
		{{Key: "$unionWith", Value: bson.D{
			{Key: "coll", Value: mongodb.MempoolPinsCollection},
			{Key: "pipeline", Value: mongo.Pipeline{
				{{Key: "$match", Value: buildMempoolFeedFilter(filter)}},
				{{Key: "$sort", Value: bson.D{{Key: "_id", Value: -1}}}},
				buildConfirmedTweetLookupStage(),
				{{Key: "$match", Value: bson.D{{Key: "confirmedTweet.0", Value: bson.D{{Key: "$exists", Value: false}}}}}},
				{{Key: "$unset", Value: "confirmedTweet"}},
				{{Key: "$limit", Value: int64(10)}},
			}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: -1}}}},
		{{Key: "$limit", Value: int64(10)}},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildNewestFeedPipeline() = %#v, want %#v", got, want)
	}
}

func TestBuildHotFeedPipelineLimitsBranchesBeforeUnion(t *testing.T) {
	cursor := primitive.NewObjectID()
	filter := appendFeedCursorFilter(
		bson.D{
			{Key: "blocked", Value: false},
			{Key: "timestamp", Value: bson.D{{Key: "$gt", Value: int64(1779595551)}, {Key: "$lt", Value: int64(1779681951)}}},
		},
		cursor,
		"hot",
		7,
		true,
	)
	sort := bson.D{{Key: "hot", Value: -1}, {Key: "_id", Value: -1}}

	got := buildHotFeedPipeline(filter, 10)

	want := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$sort", Value: sort}},
		{{Key: "$limit", Value: int64(10)}},
		{{Key: "$unionWith", Value: bson.D{
			{Key: "coll", Value: mongodb.MempoolPinsCollection},
			{Key: "pipeline", Value: mongo.Pipeline{
				{{Key: "$match", Value: buildMempoolFeedFilter(filter)}},
				{{Key: "$sort", Value: sort}},
				buildConfirmedTweetLookupStage(),
				{{Key: "$match", Value: bson.D{{Key: "confirmedTweet.0", Value: bson.D{{Key: "$exists", Value: false}}}}}},
				{{Key: "$unset", Value: "confirmedTweet"}},
				{{Key: "$limit", Value: int64(10)}},
			}},
		}}},
		{{Key: "$sort", Value: sort}},
		{{Key: "$limit", Value: int64(10)}},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildHotFeedPipeline() = %#v, want %#v", got, want)
	}
}

func TestAppendFeedCursorFilterHotUsesCompoundSortCursor(t *testing.T) {
	cursor := primitive.NewObjectID()
	base := bson.D{{Key: "blocked", Value: false}}

	got := appendFeedCursorFilter(base, cursor, "hot", 7, true)

	want := bson.D{
		{Key: "blocked", Value: false},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "hot", Value: bson.D{{Key: "$lt", Value: 7}}}},
			bson.D{
				{Key: "hot", Value: 7},
				{Key: "_id", Value: bson.D{{Key: "$lt", Value: cursor}}},
			},
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("appendFeedCursorFilter() = %#v, want %#v", got, want)
	}
}

func TestAppendFeedCursorFilterNewestUsesIdCursor(t *testing.T) {
	cursor := primitive.NewObjectID()
	base := bson.D{{Key: "blocked", Value: false}}

	got := appendFeedCursorFilter(base, cursor, "_id", 0, false)

	want := bson.D{
		{Key: "blocked", Value: false},
		{Key: "_id", Value: bson.D{{Key: "$lt", Value: cursor}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("appendFeedCursorFilter() = %#v, want %#v", got, want)
	}
}

func TestAppendFeedCursorFilterHotFallsBackToIdCursorWhenCursorHotMissing(t *testing.T) {
	cursor := primitive.NewObjectID()
	base := bson.D{{Key: "blocked", Value: false}}

	got := appendFeedCursorFilter(base, cursor, "hot", 0, false)

	want := bson.D{
		{Key: "blocked", Value: false},
		{Key: "_id", Value: bson.D{{Key: "$lt", Value: cursor}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("appendFeedCursorFilter() = %#v, want %#v", got, want)
	}
}

func TestBuildMempoolFeedFilterCombinesPathAndCursorOrWithAnd(t *testing.T) {
	cursor := primitive.NewObjectID()
	feedFilter := appendFeedCursorFilter(bson.D{{Key: "blocked", Value: false}}, cursor, "hot", 7, true)

	got := buildMempoolFeedFilter(feedFilter)

	want := bson.D{{Key: "$and", Value: bson.A{DataFilter, feedFilter}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildMempoolFeedFilter() = %#v, want %#v", got, want)
	}
}

func TestBuildVisibleMempoolFeedPipelineExcludesConfirmedTweetsBeforeLimit(t *testing.T) {
	filter := bson.D{{Key: "blocked", Value: false}}
	sort := bson.D{{Key: "_id", Value: -1}}

	got := buildVisibleMempoolFeedPipeline(filter, sort, 10)

	wantPrefix := mongo.Pipeline{
		{{Key: "$match", Value: buildMempoolFeedFilter(filter)}},
		{{Key: "$sort", Value: sort}},
		buildConfirmedTweetLookupStage(),
		{{Key: "$match", Value: bson.D{{Key: "confirmedTweet.0", Value: bson.D{{Key: "$exists", Value: false}}}}}},
		{{Key: "$unset", Value: "confirmedTweet"}},
		{{Key: "$limit", Value: int64(10)}},
	}
	if !reflect.DeepEqual(got, wantPrefix) {
		t.Fatalf("buildVisibleMempoolFeedPipeline() = %#v, want %#v", got, wantPrefix)
	}
}

func TestBuildConfirmedTweetLookupStageUsesExistenceOnlyLookup(t *testing.T) {
	got := buildConfirmedTweetLookupStage()

	want := bson.D{{Key: "$lookup", Value: bson.D{
		{Key: "from", Value: TweetCollection},
		{Key: "let", Value: bson.D{{Key: "pinId", Value: "$id"}}},
		{Key: "pipeline", Value: mongo.Pipeline{
			{{Key: "$match", Value: bson.D{{Key: "$expr", Value: bson.D{{Key: "$eq", Value: bson.A{"$id", "$$pinId"}}}}}}},
			{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
			{{Key: "$limit", Value: int64(1)}},
		}},
		{Key: "as", Value: "confirmedTweet"},
	}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildConfirmedTweetLookupStage() = %#v, want %#v", got, want)
	}
}
