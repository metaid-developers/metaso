package metaso

import "testing"

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
