package man

import (
	"testing"

	"manindexer/pin"
)

func TestModifyPinStatusAllowsAlreadyModifiedAndSameHeight(t *testing.T) {
	original := &pin.PinInscription{
		Id:            "createPinId",
		Address:       "addr-1",
		Operation:     "create",
		Status:        1,
		GenesisHeight: 100,
	}
	modify := &pin.PinInscription{
		Id:            "modifyPinId",
		Address:       "addr-1",
		Operation:     "modify",
		GenesisHeight: 100,
	}

	if got := ModifyPinStatus(modify, original); got != 0 {
		t.Fatalf("ModifyPinStatus() = %d, want 0 for subsequent same-height modify", got)
	}
}

func TestModifyPinStatusRejectsMissingOriginalAndWrongAddress(t *testing.T) {
	modify := &pin.PinInscription{
		Id:            "modifyPinId",
		Address:       "attacker",
		Operation:     "modify",
		GenesisHeight: 2,
	}
	if got := ModifyPinStatus(modify, nil); got != StatusModifyPinIdNotExist {
		t.Fatalf("missing original status = %d, want %d", got, StatusModifyPinIdNotExist)
	}

	original := &pin.PinInscription{
		Id:            "createPinId",
		Address:       "owner",
		Operation:     "create",
		GenesisHeight: 1,
	}
	if got := ModifyPinStatus(modify, original); got != StatusModifyPinAddrDenied {
		t.Fatalf("wrong address status = %d, want %d", got, StatusModifyPinAddrDenied)
	}
}

func TestGetModifyPinStatusAllowsSecondModify(t *testing.T) {
	original := &pin.PinInscription{
		Id:            "createPinId",
		Address:       "addr-1",
		Operation:     "create",
		Status:        1,
		GenesisHeight: 10,
	}
	modify := &pin.PinInscription{
		Id:            "modify-2",
		Address:       "addr-1",
		Operation:     "modify",
		OriginalId:    "createPinId",
		GenesisHeight: 12,
	}

	statusMap := getModifyPinStatus(
		map[string]*pin.PinInscription{modify.Id: modify},
		map[string]*pin.PinInscription{original.Id: original},
	)
	if code, ok := statusMap[modify.Id]; ok && code < 0 {
		t.Fatalf("second modify status = %d, want legal", code)
	}
}

func TestPrepareOriginalPinUpdatesKeepsLatestContent(t *testing.T) {
	original := &pin.PinInscription{
		Id:            "createPinId",
		Path:          "/protocols/simplenote",
		ModifyHistory: []string{"createPinId"},
	}
	first := &pin.PinInscription{
		Id:            "modify-1",
		OriginalId:    "createPinId",
		Status:        1,
		GenesisHeight: 11,
		TxIndex:       1,
		ContentBody:   []byte("mid"),
	}
	second := &pin.PinInscription{
		Id:            "modify-2",
		OriginalId:    "createPinId",
		Status:        1,
		GenesisHeight: 12,
		TxIndex:       3,
		ContentBody:   []byte("latest"),
	}

	got := PrepareOriginalPinUpdates([]*pin.PinInscription{first, second}, map[string]*pin.PinInscription{
		original.Id: original,
	})
	if len(got) != 1 {
		t.Fatalf("updates = %d, want 1 collapsed modify", len(got))
	}
	if string(got[0].ContentBody) != "latest" {
		t.Fatalf("collapsed body = %q, want latest", got[0].ContentBody)
	}
	if got[0].OriginalPath != "/protocols/simplenote" {
		t.Fatalf("OriginalPath = %q, want /protocols/simplenote", got[0].OriginalPath)
	}
	if len(got[0].ModifyHistory) != 3 {
		t.Fatalf("ModifyHistory = %#v, want original + both modifies", got[0].ModifyHistory)
	}
}
