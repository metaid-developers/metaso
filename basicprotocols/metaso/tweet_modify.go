package metaso

import (
	"context"
	"log"
	"strings"

	"manindexer/database/mongodb"
	"manindexer/man"
	"manindexer/pin"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const tweetModifySyncKey = "tweet_modify"
const pinModifyContentBackfillFlag = "pin_modify_content_backfill"

// agentpediaTutorialPinID is the production simplenote that stayed on its
// genesis draft after two legal modifies. Replay it first so the public page
// recovers even if the full historical scan is interrupted.
const agentpediaTutorialPinID = "36e4da6b874f5001df34c82c8b10391c54d8a8a1c89d510a4507f3a114660530i0"

func shouldInsertTweet(doc *Tweet) bool {
	if doc == nil {
		return false
	}
	if doc.Operation == "modify" || doc.Operation == "revoke" || doc.Operation == "hide" {
		return false
	}
	if strings.HasPrefix(doc.Path, "@") {
		return false
	}
	return pin.IsBuzzProtocolPath(doc.Path)
}

func tweetContentUpdate(doc *Tweet) bson.M {
	return bson.M{
		"contentbody":       doc.ContentBody,
		"contentlength":     doc.ContentLength,
		"contenttype":       doc.ContentType,
		"contenttypedetect": doc.ContentTypeDetect,
		"contentsummary":    doc.ContentSummary,
		"keywords":          buzzKeywords(doc),
	}
}

func tweetAsPin(doc *Tweet) *pin.PinInscription {
	if doc == nil {
		return nil
	}
	return &pin.PinInscription{
		Id:                doc.Id,
		Address:           doc.Address,
		Operation:         doc.Operation,
		Status:            doc.Status,
		IsTransfered:      doc.IsTransfered,
		GenesisHeight:     doc.GenesisHeight,
		TxIndex:           doc.TxIndex,
		Path:              doc.Path,
		OriginalPath:      doc.OriginalPath,
		OriginalId:        doc.OriginalId,
		ContentBody:       doc.ContentBody,
		ContentLength:     doc.ContentLength,
		ContentType:       doc.ContentType,
		ContentTypeDetect: doc.ContentTypeDetect,
		ContentSummary:    doc.ContentSummary,
		ModifyHistory:     nil,
	}
}

func applyConfirmedModifyToTweet(modify *Tweet) error {
	if modify == nil {
		return nil
	}
	originalId := pin.OriginalIdFromModify(modify.Path, modify.OriginalId)
	if originalId == "" {
		return nil
	}
	original, err := loadPinTweet(originalId)
	if err != nil || original == nil {
		return err
	}
	original = walkToGenesisTweet(original)
	if original == nil {
		return nil
	}
	if code := man.ModifyPinStatus(tweetAsPin(modify), tweetAsPin(original)); code != 0 {
		return nil
	}
	path := pin.BuzzPathOfOriginal(tweetAsPin(original))
	if !pin.IsBuzzProtocolPath(path) {
		return nil
	}
	updateDoc := &Tweet{
		Id:                original.Id,
		Path:              path,
		ContentBody:       modify.ContentBody,
		ContentLength:     modify.ContentLength,
		ContentType:       modify.ContentType,
		ContentTypeDetect: modify.ContentTypeDetect,
		ContentSummary:    modify.ContentSummary,
	}
	_, err = mongoClient.Collection(TweetCollection).UpdateOne(
		context.TODO(),
		bson.M{"id": original.Id},
		bson.M{"$set": tweetContentUpdate(updateDoc)},
	)
	if err == mongo.ErrNoDocuments {
		return nil
	}
	return err
}

func loadPinTweet(id string) (*Tweet, error) {
	var doc Tweet
	err := mongoClient.Collection(mongodb.PinsCollection).FindOne(context.TODO(), bson.M{"id": id}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

func walkToGenesisTweet(doc *Tweet) *Tweet {
	current := doc
	for i := 0; i < 500 && current != nil && current.Operation == "modify"; i++ {
		nextId := pin.OriginalIdFromModify(current.Path, current.OriginalId)
		if nextId == "" || nextId == current.Id {
			break
		}
		next, err := loadPinTweet(nextId)
		if err != nil || next == nil {
			break
		}
		current = next
	}
	return current
}

func (metaso *MetaSo) synchTweetModify() (err error) {
	last, err := mongodb.GetSyncLastId(tweetModifySyncKey)
	if err != nil {
		return
	}
	filter := bson.D{{Key: "operation", Value: "modify"}}
	if last != primitive.NilObjectID {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$gt", Value: last}}})
	}
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{Key: "_id", Value: 1}})
	findOptions.SetLimit(500)
	result, err := mongoClient.Collection(mongodb.PinsCollection).Find(context.TODO(), filter, findOptions)
	if err != nil {
		return
	}
	var pinList []*Tweet
	if err = result.All(context.TODO(), &pinList); err != nil {
		return
	}
	if len(pinList) == 0 {
		return
	}
	var lastId primitive.ObjectID
	for _, doc := range pinList {
		if err = applyConfirmedModifyToTweet(doc); err != nil {
			log.Printf("synchTweetModify apply %s: %v", doc.Id, err)
			return
		}
		if mongodb.CompareObjectIDs(doc.MogoID, lastId) > 0 {
			lastId = doc.MogoID
		}
	}
	if lastId != primitive.NilObjectID {
		err = mongodb.UpdateSyncLastIdLog(tweetModifySyncKey, lastId)
	}
	return
}

func (metaso *MetaSo) backfillPinModifyContent() {
	done, err := mongodb.GetSyncLastNumber(pinModifyContentBackfillFlag)
	if err != nil {
		return
	}
	if done == 1 {
		return
	}
	if err := replayHistoricalPinModifies(agentpediaTutorialPinID); err != nil {
		log.Printf("backfillPinModifyContent target fail: %v", err)
		return
	}
	if err := replayHistoricalPinModifies(""); err != nil {
		log.Printf("backfillPinModifyContent fail: %v", err)
		return
	}
	advanceTweetModifyCursorToLatest()
	mongodb.UpdateSyncLastNumber(pinModifyContentBackfillFlag, 1)
	log.Printf("backfillPinModifyContent done")
}

func advanceTweetModifyCursorToLatest() {
	opts := options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})
	var latest Tweet
	err := mongoClient.Collection(mongodb.PinsCollection).FindOne(context.TODO(), bson.M{"operation": "modify"}, opts).Decode(&latest)
	if err != nil || latest.MogoID == primitive.NilObjectID {
		return
	}
	mongodb.UpdateSyncLastIdLog(tweetModifySyncKey, latest.MogoID)
}

func replayHistoricalPinModifies(onlyOriginalId string) error {
	filter := bson.M{"operation": "modify"}
	if onlyOriginalId != "" {
		filter["$or"] = bson.A{
			bson.M{"originalid": onlyOriginalId},
			bson.M{"path": "@" + onlyOriginalId},
		}
	}
	opts := options.Find().SetSort(bson.D{
		{Key: "genesisheight", Value: 1},
		{Key: "txindex", Value: 1},
		{Key: "id", Value: 1},
	})
	cursor, err := mongoClient.Collection(mongodb.PinsCollection).Find(context.TODO(), filter, opts)
	if err != nil {
		return err
	}
	var modifies []*pin.PinInscription
	if err = cursor.All(context.TODO(), &modifies); err != nil {
		return err
	}
	mg := &mongodb.Mongodb{}
	applied := 0
	for _, modify := range modifies {
		payload := buildLegalModifyPayload(modify)
		if payload == nil {
			continue
		}
		if err = mg.BatchUpdatePins([]*pin.PinInscription{payload}); err != nil {
			return err
		}
		if man.PebbleStore != nil && man.PebbleStore.Database != nil {
			if err = man.PebbleStore.Database.BatchUpdatePins([]*pin.PinInscription{payload}); err != nil {
				log.Printf("backfillPinModifyContent pebble %s: %v", payload.OriginalId, err)
			}
		}
		if err = applyConfirmedModifyToTweet(pinAsTweet(modify)); err != nil {
			return err
		}
		applied++
	}
	log.Printf("replayHistoricalPinModifies applied=%d scanned=%d", applied, len(modifies))
	return nil
}

func buildLegalModifyPayload(modify *pin.PinInscription) *pin.PinInscription {
	if modify == nil {
		return nil
	}
	originalId := pin.OriginalIdFromModify(modify.Path, modify.OriginalId)
	if originalId == "" {
		return nil
	}
	original, err := loadPinInscription(originalId)
	if err != nil || original == nil {
		return nil
	}
	original = walkToGenesisPin(original)
	if original == nil {
		return nil
	}
	if code := man.ModifyPinStatus(modify, original); code != 0 {
		return nil
	}
	update := *modify
	update.Status = 1
	update.OriginalId = original.Id
	update.OriginalPath = pin.BuzzPathOfOriginal(original)
	update.ModifyHistory = pin.AppendModifyHistory(original.ModifyHistory, original.Id, modify.Id)
	return &update
}

func loadPinInscription(id string) (*pin.PinInscription, error) {
	var doc pin.PinInscription
	err := mongoClient.Collection(mongodb.PinsCollection).FindOne(context.TODO(), bson.M{"id": id}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

func walkToGenesisPin(doc *pin.PinInscription) *pin.PinInscription {
	current := doc
	for i := 0; i < 500 && current != nil && current.Operation == "modify"; i++ {
		nextId := pin.OriginalIdFromModify(current.Path, current.OriginalId)
		if nextId == "" || nextId == current.Id {
			break
		}
		next, err := loadPinInscription(nextId)
		if err != nil || next == nil {
			break
		}
		current = next
	}
	return current
}

func pinAsTweet(p *pin.PinInscription) *Tweet {
	if p == nil {
		return nil
	}
	return &Tweet{
		Id:                p.Id,
		Address:           p.Address,
		Operation:         p.Operation,
		Status:            p.Status,
		IsTransfered:      p.IsTransfered,
		GenesisHeight:     p.GenesisHeight,
		TxIndex:           p.TxIndex,
		Path:              p.Path,
		OriginalPath:      p.OriginalPath,
		OriginalId:        p.OriginalId,
		ContentBody:       p.ContentBody,
		ContentLength:     p.ContentLength,
		ContentType:       p.ContentType,
		ContentTypeDetect: p.ContentTypeDetect,
		ContentSummary:    p.ContentSummary,
	}
}
