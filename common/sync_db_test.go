package common

import (
	"errors"
	"testing"
)

func TestSyncDBAccessWithoutInitReturnsError(t *testing.T) {
	db = nil

	if err := SaveToDictDB("check_height_btc", []byte("1")); !errors.Is(err, ErrSyncDBNotInitialized) {
		t.Fatalf("SaveToDictDB() error = %v, want %v", err, ErrSyncDBNotInitialized)
	}

	data, err := LoadFromDictDB("check_height_btc")
	if !errors.Is(err, ErrSyncDBNotInitialized) {
		t.Fatalf("LoadFromDictDB() error = %v, want %v", err, ErrSyncDBNotInitialized)
	}
	if data != nil {
		t.Fatalf("LoadFromDictDB() data = %q, want nil", data)
	}
}
