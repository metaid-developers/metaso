package mongodb

import (
	"errors"
	"testing"

	"manindexer/pin"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestIgnoreDuplicateKeyBulkWriteErrorIgnoresOnlyDuplicateKeys(t *testing.T) {
	err := ignoreDuplicateKeyBulkWriteError(mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Code: 11000}},
			{WriteError: mongo.WriteError{Code: 11000}},
		},
	})

	if err != nil {
		t.Fatalf("ignoreDuplicateKeyBulkWriteError() error = %v, want nil", err)
	}
}

func TestIgnoreDuplicateKeyBulkWriteErrorReturnsMixedErrors(t *testing.T) {
	original := mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Code: 11000}},
			{WriteError: mongo.WriteError{Code: 121}},
		},
	}

	err := ignoreDuplicateKeyBulkWriteError(original)

	if err == nil {
		t.Fatal("ignoreDuplicateKeyBulkWriteError() error = nil, want original error")
	}
}

func TestIgnoreDuplicateKeyBulkWriteErrorReturnsNonBulkErrors(t *testing.T) {
	original := errors.New("network failed")

	err := ignoreDuplicateKeyBulkWriteError(original)

	if !errors.Is(err, original) {
		t.Fatalf("ignoreDuplicateKeyBulkWriteError() error = %v, want %v", err, original)
	}
}

func TestPinUpdateWriteModelWritesContentOnLegalModify(t *testing.T) {
	model := pinUpdateWriteModel(&pin.PinInscription{
		Id:                "modifyPinId",
		OriginalId:        "createPinId",
		Address:           "addr-1",
		Status:            1,
		ContentBody:       []byte("latest-body"),
		ContentLength:     11,
		ContentType:       "application/json",
		ContentTypeDetect: "text/plain; charset=utf-8",
		ContentSummary:    "latest-body",
		ModifyHistory:     []string{"createPinId", "modifyPinId"},
	})
	if model == nil {
		t.Fatal("pinUpdateWriteModel() = nil, want update model")
	}
	updateModel, ok := model.(*mongo.UpdateOneModel)
	if !ok {
		t.Fatalf("model type = %T, want *mongo.UpdateOneModel", model)
	}
	filter, _ := updateModel.Filter.(bson.D)
	if len(filter) != 2 || filter[0].Key != "id" || filter[0].Value != "createPinId" {
		t.Fatalf("filter = %#v, want original pin id", updateModel.Filter)
	}
	update, _ := updateModel.Update.(bson.D)
	if len(update) != 1 || update[0].Key != "$set" {
		t.Fatalf("update = %#v, want $set", updateModel.Update)
	}
	set, _ := update[0].Value.(bson.D)
	fields := map[string]interface{}{}
	for _, item := range set {
		fields[item.Key] = item.Value
	}
	if string(fields["contentbody"].([]byte)) != "latest-body" {
		t.Fatalf("contentbody = %v, want latest-body", fields["contentbody"])
	}
	if fields["contentlength"] != uint64(11) {
		t.Fatalf("contentlength = %v, want 11", fields["contentlength"])
	}
	if fields["status"] != 1 {
		t.Fatalf("status = %v, want 1", fields["status"])
	}
}

func TestPinUpdateWriteModelRevokeDoesNotWriteContent(t *testing.T) {
	model := pinUpdateWriteModel(&pin.PinInscription{
		Id:          "revokePinId",
		OriginalId:  "createPinId",
		Address:     "addr-1",
		Status:      -1,
		ContentBody: []byte("revoke-payload"),
	})
	if model == nil {
		t.Fatal("pinUpdateWriteModel() = nil, want status-only update")
	}
	updateModel := model.(*mongo.UpdateOneModel)
	update := updateModel.Update.(bson.D)
	set := update[0].Value.(bson.D)
	for _, item := range set {
		if item.Key == "contentbody" {
			t.Fatal("revoke update must not overwrite contentbody")
		}
	}
}
