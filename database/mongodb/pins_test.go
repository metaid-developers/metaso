package mongodb

import (
	"errors"
	"testing"

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
