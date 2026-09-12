package pin

import "testing"

func TestOriginalIdFromModifyStripsAtPrefix(t *testing.T) {
	got := OriginalIdFromModify("@createPinId", "")
	if got != "createPinId" {
		t.Fatalf("OriginalIdFromModify() = %q, want createPinId", got)
	}
}

func TestOriginalIdFromModifyPrefersExplicitId(t *testing.T) {
	got := OriginalIdFromModify("@other", "createPinId")
	if got != "createPinId" {
		t.Fatalf("OriginalIdFromModify() = %q, want createPinId", got)
	}
}

func TestApplyModifyContentReplacesBodyAndKeepsHistory(t *testing.T) {
	original := &PinInscription{
		Id:            "createPinId",
		ContentBody:   []byte("short"),
		ContentLength: 5,
		ContentType:   "text/plain",
		ContentSummary: "short",
	}
	modify := &PinInscription{
		Id:                 "modifyPinId",
		ContentBody:        []byte("much longer body"),
		ContentLength:      16,
		ContentType:        "application/json",
		ContentTypeDetect:  "text/plain; charset=utf-8",
		ContentSummary:     "much longer body",
	}

	ApplyModifyContent(original, modify)

	if string(original.ContentBody) != "much longer body" {
		t.Fatalf("ContentBody = %q, want much longer body", original.ContentBody)
	}
	if original.ContentLength != 16 {
		t.Fatalf("ContentLength = %d, want 16", original.ContentLength)
	}
	if original.ContentType != "application/json" {
		t.Fatalf("ContentType = %q, want application/json", original.ContentType)
	}
	if original.Status != 1 {
		t.Fatalf("Status = %d, want 1", original.Status)
	}
	if len(original.ModifyHistory) != 2 || original.ModifyHistory[0] != "createPinId" || original.ModifyHistory[1] != "modifyPinId" {
		t.Fatalf("ModifyHistory = %#v, want [createPinId modifyPinId]", original.ModifyHistory)
	}
}

func TestIsBuzzProtocolPathIncludesSimpleNote(t *testing.T) {
	if !IsBuzzProtocolPath("/protocols/simplenote") {
		t.Fatal("simplenote should be a buzz protocol path")
	}
	if IsBuzzProtocolPath("@createPinId") {
		t.Fatal("modify path must not be treated as a buzz protocol")
	}
}
