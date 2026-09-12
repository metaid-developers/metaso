package man

import (
	"manindexer/pin"
)

func ModifyPinStatus(modify, original *pin.PinInscription) int {
	if original == nil {
		return StatusModifyPinIdNotExist
	}
	if modify == nil {
		return StatusModifyPinIdNotExist
	}
	if modify.Address != original.Address {
		return StatusModifyPinAddrDenied
	}
	if original.Operation == "init" {
		return StatusModifyPinOptIsInit
	}
	if original.Status == -1 {
		return StatusRevokePinIsRevoked
	}
	if original.IsTransfered {
		return StatusPinIsTransfered
	}
	if modify.GenesisHeight < original.GenesisHeight {
		return StatusBlockHeightLower
	}
	return 0
}

func MergeOriginalPinMap(pinList []interface{}, stored []*pin.PinInscription) map[string]*pin.PinInscription {
	m := make(map[string]*pin.PinInscription, len(pinList)+len(stored))
	for _, item := range pinList {
		p, ok := item.(*pin.PinInscription)
		if !ok || p == nil || p.Id == "" {
			continue
		}
		m[p.Id] = p
	}
	for _, p := range stored {
		if p == nil || p.Id == "" {
			continue
		}
		if _, exists := m[p.Id]; exists {
			continue
		}
		m[p.Id] = p
	}
	return m
}

func PrepareOriginalPinUpdates(updates []*pin.PinInscription, originalPinMap map[string]*pin.PinInscription) []*pin.PinInscription {
	type group struct {
		latest *pin.PinInscription
		ids    []string
	}
	groups := make(map[string]*group)
	out := make([]*pin.PinInscription, 0, len(updates))
	for _, u := range updates {
		if u == nil {
			continue
		}
		if u.Status < 0 {
			out = append(out, u)
			continue
		}
		g, ok := groups[u.OriginalId]
		if !ok {
			g = &group{}
			groups[u.OriginalId] = g
		}
		if g.latest == nil || pin.ComparePinChainOrder(g.latest, u) < 0 {
			g.latest = u
		}
		g.ids = pin.AppendModifyHistory(g.ids, "", u.Id)
	}
	for originalId, g := range groups {
		if g.latest == nil {
			continue
		}
		var history []string
		if original, ok := originalPinMap[originalId]; ok && original != nil {
			history = append([]string{}, original.ModifyHistory...)
			g.latest.OriginalPath = pin.BuzzPathOfOriginal(original)
		}
		history = pin.AppendModifyHistory(history, originalId, "")
		for _, id := range g.ids {
			history = pin.AppendModifyHistory(history, "", id)
		}
		g.latest.ModifyHistory = history
		out = append(out, g.latest)
	}
	return out
}
