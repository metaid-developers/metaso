package metaso

import (
	"testing"

	"manindexer/man"
	"manindexer/pin"
)

type localNode struct {
	pins   map[string]*pin.PinInscription
	tweets map[string]*Tweet
}

func newLocalNode() *localNode {
	return &localNode{
		pins:   map[string]*pin.PinInscription{},
		tweets: map[string]*Tweet{},
	}
}

func clonePin(src *pin.PinInscription) *pin.PinInscription {
	cp := *src
	cp.ContentBody = append([]byte(nil), src.ContentBody...)
	if src.ModifyHistory != nil {
		cp.ModifyHistory = append([]string{}, src.ModifyHistory...)
	}
	return &cp
}

func (n *localNode) confirm(pins ...*pin.PinInscription) {
	stored := make([]*pin.PinInscription, 0, len(n.pins))
	for _, p := range n.pins {
		stored = append(stored, p)
	}
	list := make([]interface{}, 0, len(pins))
	for _, p := range pins {
		cp := clonePin(p)
		n.pins[cp.Id] = cp
		list = append(list, cp)
	}
	originalMap := man.MergeOriginalPinMap(list, stored)
	var updates []*pin.PinInscription
	for _, item := range list {
		p := item.(*pin.PinInscription)
		if p.Operation != "modify" && p.Operation != "revoke" {
			continue
		}
		update := *p
		update.OriginalId = pin.OriginalIdFromModify(p.Path, p.OriginalId)
		if p.Operation == "modify" {
			update.Status = 1
			if code := man.ModifyPinStatus(&update, originalMap[update.OriginalId]); code != 0 {
				p.Status = code
				continue
			}
		} else {
			update.Status = -1
		}
		updates = append(updates, &update)
	}
	for _, u := range man.PrepareOriginalPinUpdates(updates, originalMap) {
		orig := n.pins[u.OriginalId]
		if orig == nil {
			continue
		}
		if u.Status == 1 {
			pin.ApplyModifyContent(orig, u)
			continue
		}
		orig.Status = u.Status
	}
}

func (n *localNode) syncTweets() {
	for _, p := range n.pins {
		doc := pinAsTweet(p)
		if !shouldInsertTweet(doc) {
			continue
		}
		if _, exists := n.tweets[doc.Id]; exists {
			continue
		}
		n.tweets[doc.Id] = doc
	}
}

func (n *localNode) syncTweetModifies() {
	for _, p := range n.pins {
		if p.Operation != "modify" {
			continue
		}
		originalId := pin.OriginalIdFromModify(p.Path, p.OriginalId)
		original := n.pins[originalId]
		if original == nil {
			continue
		}
		if code := man.ModifyPinStatus(p, original); code != 0 {
			continue
		}
		path := pin.BuzzPathOfOriginal(original)
		if !pin.IsBuzzProtocolPath(path) {
			continue
		}
		tweet, ok := n.tweets[original.Id]
		if !ok {
			continue
		}
		tweet.ContentBody = append([]byte(nil), p.ContentBody...)
		tweet.ContentLength = p.ContentLength
		tweet.ContentType = p.ContentType
		tweet.ContentTypeDetect = p.ContentTypeDetect
		tweet.ContentSummary = p.ContentSummary
		tweet.Content = string(tweet.ContentBody)
	}
}

func (n *localNode) content(id string) string {
	p := n.pins[id]
	if p == nil {
		return ""
	}
	return string(p.ContentBody)
}

func (n *localNode) buzzInfo(id string) *Tweet {
	tweet := n.tweets[id]
	if tweet == nil {
		return nil
	}
	cp := *tweet
	cp.Content = string(cp.ContentBody)
	return &cp
}

func (n *localNode) timelineIDs() []string {
	ids := make([]string, 0, len(n.tweets))
	for id := range n.tweets {
		ids = append(ids, id)
	}
	return ids
}

func TestSimpleNoteModifyUpdatesContentAndKeepsSingleTimelineItem(t *testing.T) {
	const createPinId = "createPinId"
	shortBody := []byte(`{"title":"note","content":"short genesis"}`)
	longBody := []byte(`{"title":"note","content":"修宪要走两阶段投票 最后一句"}`)

	node := newLocalNode()
	node.confirm(&pin.PinInscription{
		Id:            createPinId,
		Address:       "addr-1",
		Operation:     "create",
		Path:          "/protocols/simplenote",
		OriginalPath:  "/protocols/simplenote",
		ContentBody:   shortBody,
		ContentLength: uint64(len(shortBody)),
		ContentType:   "application/json",
		GenesisHeight: 10,
	})
	node.syncTweets()

	if got := node.content(createPinId); got != string(shortBody) {
		t.Fatalf("create /content = %q, want short genesis", got)
	}

	node.confirm(&pin.PinInscription{
		Id:            "modifyPinId",
		Address:       "addr-1",
		Operation:     "modify",
		Path:          "@" + createPinId,
		ContentBody:   longBody,
		ContentLength: uint64(len(longBody)),
		ContentType:   "application/json",
		GenesisHeight: 12,
	})
	node.syncTweets()
	node.syncTweetModifies()

	if got := node.content(createPinId); got != string(longBody) {
		t.Fatalf("/content/%s = %q, want long modified body", createPinId, got)
	}
	info := node.buzzInfo(createPinId)
	if info == nil {
		t.Fatal("/social/buzz/info returned no tweet")
	}
	if info.Id != createPinId {
		t.Fatalf("tweet.id = %q, want create pin id", info.Id)
	}
	if info.Content != string(longBody) {
		t.Fatalf("tweet.content = %q, want long modified body", info.Content)
	}
	if ids := node.timelineIDs(); len(ids) != 1 || ids[0] != createPinId {
		t.Fatalf("timeline ids = %#v, want only %s", ids, createPinId)
	}
}

func TestIllegalModifyDoesNotOverwriteContent(t *testing.T) {
	const createPinId = "createPinId"
	shortBody := []byte("short genesis")
	node := newLocalNode()
	node.confirm(&pin.PinInscription{
		Id:            createPinId,
		Address:       "owner",
		Operation:     "create",
		Path:          "/protocols/simplenote",
		OriginalPath:  "/protocols/simplenote",
		ContentBody:   shortBody,
		ContentLength: uint64(len(shortBody)),
		GenesisHeight: 10,
	})
	node.syncTweets()

	node.confirm(&pin.PinInscription{
		Id:            "bad-address",
		Address:       "attacker",
		Operation:     "modify",
		Path:          "@" + createPinId,
		ContentBody:   []byte("stolen rewrite"),
		ContentLength: 14,
		GenesisHeight: 12,
	})
	node.confirm(&pin.PinInscription{
		Id:            "missing-original",
		Address:       "owner",
		Operation:     "modify",
		Path:          "@does-not-exist",
		ContentBody:   []byte("ghost rewrite"),
		ContentLength: 13,
		GenesisHeight: 13,
	})
	node.syncTweets()
	node.syncTweetModifies()

	if got := node.content(createPinId); got != string(shortBody) {
		t.Fatalf("content after illegal modify = %q, want original", got)
	}
	info := node.buzzInfo(createPinId)
	if info == nil || info.Content != string(shortBody) {
		t.Fatalf("buzz info after illegal modify = %#v, want original", info)
	}
	if ids := node.timelineIDs(); len(ids) != 1 || ids[0] != createPinId {
		t.Fatalf("timeline ids = %#v, want only create pin", ids)
	}
}

func TestShouldInsertTweetRejectsModifyPath(t *testing.T) {
	if shouldInsertTweet(&Tweet{Id: "m1", Operation: "modify", Path: "@createPinId"}) {
		t.Fatal("modify operation must not be inserted as a tweet")
	}
	if shouldInsertTweet(&Tweet{Id: "m2", Operation: "create", Path: "@createPinId"}) {
		t.Fatal("path=@pinId must not be inserted as a tweet")
	}
	if !shouldInsertTweet(&Tweet{Id: "c1", Operation: "create", Path: "/protocols/simplenote"}) {
		t.Fatal("simplenote create should be inserted as a tweet")
	}
}
