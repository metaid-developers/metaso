package man

import (
	"testing"

	"manindexer/pin"
)

func TestGetStringFromMap(t *testing.T) {
	tests := []struct {
		name     string
		dataMap  map[string]interface{}
		key      string
		expected string
	}{
		{
			name:     "string value",
			dataMap:  map[string]interface{}{"key": "value"},
			key:      "key",
			expected: "value",
		},
		{
			name:     "float64 value",
			dataMap:  map[string]interface{}{"isLike": float64(1)},
			key:      "isLike",
			expected: "1",
		},
		{
			name:     "int value",
			dataMap:  map[string]interface{}{"count": 42},
			key:      "count",
			expected: "42",
		},
		{
			name:     "bool true",
			dataMap:  map[string]interface{}{"flag": true},
			key:      "flag",
			expected: "1",
		},
		{
			name:     "bool false",
			dataMap:  map[string]interface{}{"flag": false},
			key:      "flag",
			expected: "0",
		},
		{
			name:     "missing key",
			dataMap:  map[string]interface{}{"other": "value"},
			key:      "key",
			expected: "",
		},
		{
			name:     "nil value",
			dataMap:  map[string]interface{}{"key": nil},
			key:      "key",
			expected: "",
		},
		{
			name:     "empty string",
			dataMap:  map[string]interface{}{"key": ""},
			key:      "key",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getStringFromMap(tt.dataMap, tt.key)
			if result != tt.expected {
				t.Errorf("getStringFromMap() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Regression test for the production crash loop: a paylike pin whose likeTo /
// isLike arrive as JSON numbers (float64 after decoding) used to hit a raw
// `.(string)` type assertion and panic the whole indexer. The fixed code must
// convert the values and return cleanly (isLike 0 -> not a like -> no lookup).
func TestGetPayLikePinNumericFieldsNoPanic(t *testing.T) {
	pinNode := &pin.PinInscription{
		Path:        "/protocols/paylike",
		ContentBody: []byte(`{"likeTo": 123456, "isLike": 0}`),
	}
	toPIN, err := getPayLikePin(pinNode)
	if err != nil {
		t.Fatalf("getPayLikePin() returned error: %v", err)
	}
	if len(toPIN) != 0 {
		t.Errorf("getPayLikePin() = %v, want empty list for isLike != 1", toPIN)
	}
}
