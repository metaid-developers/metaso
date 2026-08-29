package mongodb

import (
	"reflect"
	"testing"
)

func TestChunkStrings(t *testing.T) {
	cases := []struct {
		name  string
		items []string
		size  int
		want  [][]string
	}{
		{"empty", nil, 1000, nil},
		{"under chunk size", []string{"a", "b"}, 1000, [][]string{{"a", "b"}}},
		{
			"exact chunks",
			[]string{"a", "b", "c", "d"},
			2,
			[][]string{{"a", "b"}, {"c", "d"}},
		},
		{
			"ragged last chunk",
			[]string{"a", "b", "c", "d", "e"},
			2,
			[][]string{{"a", "b"}, {"c", "d"}, {"e"}},
		},
		{"nonpositive size falls back to single chunk", []string{"a", "b"}, 0, [][]string{{"a", "b"}}},
	}
	for _, c := range cases {
		if got := chunkStrings(c.items, c.size); !reflect.DeepEqual(got, c.want) {
			t.Fatalf("%s: chunkStrings(%v, %d) = %v, want %v", c.name, c.items, c.size, got, c.want)
		}
	}
}
