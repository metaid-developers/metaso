package mongodb

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// MempoolPinsSweepCursorKey persists the sweep position in sync_lastid_log
	// so a restarted indexer resumes the pass instead of rescanning.
	MempoolPinsSweepCursorKey = "mempool_pins_sweep_lastid"
	// mempoolPinsSweepBatchSize pins are examined per batch; each batch does one
	// indexed $in probe against pins and at most batchSize delete operations.
	mempoolPinsSweepBatchSize = 5000
	// mempoolPinsSweepMaxBatches bounds one sweep tick so a large backlog is
	// reclaimed gradually instead of in one long write burst.
	mempoolPinsSweepMaxBatches = 10
	// mempoolPinsSweepDeleteChunk keeps each BulkWrite at the same 1000-op size
	// used by the per-block mempool cleanup helpers.
	mempoolPinsSweepDeleteChunk = 1000
)

// SweepConfirmedMempoolPins deletes mempool pins whose id already exists in the
// confirmed pins collection. The per-block cleanup only removes ids it sees in
// GetBlockTxHash(height), so a pin delivered by zmq after its confirming height
// was already cleaned (slow mempool propagation, wallet rebroadcasts) stays in
// mempoolpins forever. The sweep walks mempoolpins in _id order with a
// persisted cursor, examines up to mempoolPinsSweepMaxBatches batches per call,
// and resets the cursor after a full pass so pins confirmed later get caught by
// the next pass. Returns the number of deleted pins and whether the pass
// completed.
func (mg *Mongodb) SweepConfirmedMempoolPins() (deleted int64, completed bool) {
	for i := 0; i < mempoolPinsSweepMaxBatches; i++ {
		removed, lastID, found, err := mg.sweepConfirmedMempoolPinsBatch()
		if err != nil {
			log.Printf("SweepConfirmedMempoolPins batch fail: %v", err)
			return deleted, false
		}
		deleted += removed
		if !found {
			// Full pass done. Start over next tick: pins confirmed after this
			// pass examined them need a later pass to be removed.
			if err := UpdateSyncLastIdLog(MempoolPinsSweepCursorKey, primitive.NilObjectID); err != nil {
				log.Printf("SweepConfirmedMempoolPins reset cursor fail: %v", err)
			}
			return deleted, true
		}
		if err := UpdateSyncLastIdLog(MempoolPinsSweepCursorKey, lastID); err != nil {
			log.Printf("SweepConfirmedMempoolPins advance cursor fail: %v", err)
			return deleted, false
		}
	}
	return deleted, false
}

func (mg *Mongodb) sweepConfirmedMempoolPinsBatch() (removed int64, lastID primitive.ObjectID, found bool, err error) {
	cursorID, err := GetSyncLastId(MempoolPinsSweepCursorKey)
	if err != nil {
		return
	}
	filter := bson.M{}
	if !cursorID.IsZero() {
		filter = bson.M{"_id": bson.M{"$gt": cursorID}}
	}
	findOptions := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(int64(mempoolPinsSweepBatchSize)).
		SetProjection(bson.M{"id": 1})
	result, err := mongoClient.Collection(MempoolPinsCollection).Find(context.TODO(), filter, findOptions)
	if err != nil {
		return
	}
	type mempoolPinRef struct {
		MogoID primitive.ObjectID `bson:"_id"`
		Id     string             `bson:"id"`
	}
	var refs []*mempoolPinRef
	err = result.All(context.TODO(), &refs)
	if err != nil {
		return
	}
	if len(refs) == 0 {
		return 0, primitive.NilObjectID, false, nil
	}
	pinIds := make([]string, 0, len(refs))
	for _, ref := range refs {
		pinIds = append(pinIds, ref.Id)
	}
	confirmedIds, err := mg.findConfirmedPinIds(pinIds)
	if err != nil {
		return
	}
	if len(confirmedIds) > 0 {
		removed, err = deleteMempoolPinsByIds(confirmedIds)
		if err != nil {
			return
		}
	}
	return removed, refs[len(refs)-1].MogoID, true, nil
}

// findConfirmedPinIds returns the subset of pinIds that exist in the confirmed
// pins collection.
func (mg *Mongodb) findConfirmedPinIds(pinIds []string) (confirmedIds []string, err error) {
	filter := bson.M{"id": bson.M{"$in": pinIds}}
	findOptions := options.Find().SetProjection(bson.M{"id": 1})
	result, err := mongoClient.Collection(PinsCollection).Find(context.TODO(), filter, findOptions)
	if err != nil {
		return
	}
	type pinRef struct {
		Id string `bson:"id"`
	}
	var refs []*pinRef
	err = result.All(context.TODO(), &refs)
	if err != nil {
		return
	}
	for _, ref := range refs {
		confirmedIds = append(confirmedIds, ref.Id)
	}
	return
}

func deleteMempoolPinsByIds(pinIds []string) (deleted int64, err error) {
	for _, chunk := range chunkStrings(pinIds, mempoolPinsSweepDeleteChunk) {
		var operations []mongo.WriteModel
		for _, id := range chunk {
			operations = append(operations, mongo.NewDeleteOneModel().SetFilter(bson.M{"id": id}))
		}
		bulkWriteOptions := options.BulkWrite().SetOrdered(false)
		result, err := mongoClient.Collection(MempoolPinsCollection).BulkWrite(context.Background(), operations, bulkWriteOptions)
		if err != nil {
			log.Printf("deleteMempoolPinsByIds fail %v", err)
			continue
		}
		deleted += result.DeletedCount
	}
	return
}

func chunkStrings(items []string, size int) (chunks [][]string) {
	if size <= 0 {
		return [][]string{items}
	}
	for start := 0; start < len(items); start += size {
		end := start + size
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[start:end])
	}
	return
}

// RunMempoolPinsSweep runs SweepConfirmedMempoolPins on a ticker. Each tick
// examines up to mempoolPinsSweepMaxBatches*mempoolPinsSweepBatchSize pins, so
// a full backlog pass completes across several ticks without blocking the sync
// loops.
func RunMempoolPinsSweep(interval time.Duration) {
	mg := &Mongodb{}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		deleted, completed := mg.SweepConfirmedMempoolPins()
		if deleted > 0 || completed {
			log.Printf("mempool pins sweep deleted=%d completed=%v", deleted, completed)
		}
	}
}
