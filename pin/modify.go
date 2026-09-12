package pin

import "strings"

var BuzzProtocolPaths = map[string]struct{}{
	"/protocols/simplebuzz":    {},
	"/protocols/banana":        {},
	"/protocols/paybuzz":       {},
	"/protocols/subscribebuzz": {},
	"/protocols/simplenote":    {},
}

func IsBuzzProtocolPath(path string) bool {
	_, ok := BuzzProtocolPaths[path]
	return ok
}

func OriginalIdFromModify(path, originalId string) string {
	if originalId != "" && !strings.HasPrefix(originalId, "@") {
		return originalId
	}
	return strings.ReplaceAll(path, "@", "")
}

func BuzzPathOfOriginal(original *PinInscription) string {
	if original == nil {
		return ""
	}
	if IsBuzzProtocolPath(original.Path) {
		return original.Path
	}
	if IsBuzzProtocolPath(original.OriginalPath) {
		return original.OriginalPath
	}
	if original.Path != "" {
		return original.Path
	}
	return original.OriginalPath
}

func AppendModifyHistory(history []string, originalId, modifyId string) []string {
	if originalId != "" && !containsString(history, originalId) {
		history = append(history, originalId)
	}
	if modifyId != "" && !containsString(history, modifyId) {
		history = append(history, modifyId)
	}
	return history
}

func ApplyModifyContent(original, modify *PinInscription) {
	if original == nil || modify == nil {
		return
	}
	original.ContentBody = append([]byte(nil), modify.ContentBody...)
	original.ContentLength = modify.ContentLength
	original.ContentType = modify.ContentType
	original.ContentTypeDetect = modify.ContentTypeDetect
	original.ContentSummary = modify.ContentSummary
	original.Status = 1
	original.ModifyHistory = AppendModifyHistory(original.ModifyHistory, original.Id, modify.Id)
}

func ComparePinChainOrder(a, b *PinInscription) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	if a.GenesisHeight != b.GenesisHeight {
		if a.GenesisHeight < b.GenesisHeight {
			return -1
		}
		return 1
	}
	if a.TxIndex != b.TxIndex {
		if a.TxIndex < b.TxIndex {
			return -1
		}
		return 1
	}
	return strings.Compare(a.Id, b.Id)
}

func containsString(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}
