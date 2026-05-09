package man

import (
	"errors"
	"testing"

	"manindexer/pin"
)

type fakeIndexedPinsWriter struct {
	calls int
	pins  []interface{}
	err   error
}

func (w *fakeIndexedPinsWriter) BatchAddPins(pins []interface{}) error {
	w.calls++
	w.pins = append([]interface{}{}, pins...)
	return w.err
}

func TestWriteIndexedPinsPersistsPinsForEveryChain(t *testing.T) {
	writer := &fakeIndexedPinsWriter{}
	mvcPin := &pin.PinInscription{Id: "mvc-pin", ChainName: "mvc"}
	opcatPin := &pin.PinInscription{Id: "opcat-pin", ChainName: "opcat"}

	err := writeIndexedPins(writer, []interface{}{mvcPin, opcatPin})

	if err != nil {
		t.Fatalf("writeIndexedPins() error = %v", err)
	}
	if writer.calls != 1 {
		t.Fatalf("BatchAddPins calls = %d, want 1", writer.calls)
	}
	if len(writer.pins) != 2 {
		t.Fatalf("persisted pins = %d, want 2", len(writer.pins))
	}
	if writer.pins[0].(*pin.PinInscription).ChainName != "mvc" {
		t.Fatalf("first pin chain = %s, want mvc", writer.pins[0].(*pin.PinInscription).ChainName)
	}
	if writer.pins[1].(*pin.PinInscription).ChainName != "opcat" {
		t.Fatalf("second pin chain = %s, want opcat", writer.pins[1].(*pin.PinInscription).ChainName)
	}
}

func TestWriteIndexedPinsSkipsEmptyInput(t *testing.T) {
	writer := &fakeIndexedPinsWriter{}

	err := writeIndexedPins(writer, nil)

	if err != nil {
		t.Fatalf("writeIndexedPins() error = %v", err)
	}
	if writer.calls != 0 {
		t.Fatalf("BatchAddPins calls = %d, want 0", writer.calls)
	}
}

func TestWriteIndexedPinsReturnsWriterError(t *testing.T) {
	expected := errors.New("write failed")
	writer := &fakeIndexedPinsWriter{err: expected}
	pins := []interface{}{&pin.PinInscription{Id: "pin", ChainName: "mvc"}}

	err := writeIndexedPins(writer, pins)

	if !errors.Is(err, expected) {
		t.Fatalf("writeIndexedPins() error = %v, want %v", err, expected)
	}
}
