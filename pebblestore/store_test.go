package pebblestore

import (
	"manindexer/pin"
	"strings"
	"testing"
)

func TestBatchInsertPins(t *testing.T) {
	// 使用临时目录
	dir := t.TempDir()
	idx, err := NewDataBase(dir, 4)
	if err != nil {
		t.Fatalf("NewDataBase err: %v", err)
	}
	defer idx.Close()

	pins := []pin.PinInscription{
		{Id: "txid1", ChainName: "chainA", ContentSummary: "111"},
		{Id: "txid2", ChainName: "chainA", ContentSummary: "222"},
		{Id: "txid3", ChainName: "chainB", ContentSummary: "333"},
	}
	err = idx.BatchInsertPins(pins)
	if err != nil {
		t.Fatalf("BatchInsertPins err: %v", err)
	}

	// 查询主键（自动分片）
	key := "txid1"
	got, err := idx.GetPinInscriptionByKey(key)
	if err != nil {
		t.Fatalf("主键查询失败: %v", err)
	}
	t.Logf("主键%s查询结果: %+v", key, got)
	if got.ContentSummary != "111" {
		t.Fatalf("主键查询内容不符: %+v", got)
	}

	// 批量查询主键
	keys := []string{"txid1", "txid2", "txid3", "notfound"}
	vals := idx.BatchGetPinByKeys(keys, false)
	for _, k := range keys {
		if v, ok := vals[k]; ok {
			t.Logf("批量主键查询: %s => %s", k, string(v))
		} else {
			t.Logf("批量主键查询: %s => not found", k)
		}
	}

	// 测试区块交易表写入和读取
	blockKeys := []string{"txid1:0", "txid2:1"}
	blockKey := "chainA_block_1"
	err = idx.InsertBlockTxs(blockKey, strings.Join(blockKeys, ","))
	if err != nil {
		t.Fatalf("InsertBlockTxs err: %v", err)
	}
	val, closer, err := idx.BlocksDB.Get([]byte(blockKey))
	if err != nil {
		t.Fatalf("区块交易表查询失败: %v", err)
	}
	blockTxs := SplitBytesOptimized(string(val), ",")
	closer.Close()
	if len(blockTxs) != 2 || blockTxs[0] != "txid1:0" {
		t.Fatalf("区块交易表内容不符: %+v", blockTxs)
	}
	t.Logf("区块交易表内容: %+v", blockTxs)
}

func TestNewDataBaseUsesDefaultShardCountWhenConfigIsZero(t *testing.T) {
	idx, err := NewDataBase(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewDataBase err: %v", err)
	}
	defer idx.Close()

	if len(idx.PinsDBs) != ShardConfig {
		t.Fatalf("len(PinsDBs) = %d, want default shard count %d", len(idx.PinsDBs), ShardConfig)
	}
}

func TestPebbleMerge(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewDataBase(dir, 4)
	if err != nil {
		t.Fatalf("NewDataBase err: %v", err)
	}
	defer idx.Close()
	data := make(map[string]string)
	data["a"] = "1"
	if err := idx.BatchMergeAddressData(data); err != nil {
		t.Fatalf("BatchMergeAddressData err: %v", err)
	}
	data2 := make(map[string]string)
	data2["a"] = "2"
	if err := idx.BatchMergeAddressData(data2); err != nil {
		t.Fatalf("BatchMergeAddressData err: %v", err)
	}
	v, closer, err := idx.AddressDB.Get([]byte("a"))
	if err != nil {
		t.Fatalf("AddressDB.Get err: %v", err)
	}
	defer closer.Close()
	if string(v) != "12" {
		t.Fatalf("merged value = %q, want %q", string(v), "12")
	}
}

func TestBatchUpdatePinsWritesLatestContentToOriginalId(t *testing.T) {
	idx, err := NewDataBase(t.TempDir(), 4)
	if err != nil {
		t.Fatalf("NewDataBase err: %v", err)
	}
	defer idx.Close()

	create := pin.PinInscription{
		Id:            "createPinId",
		Address:       "addr-1",
		ContentBody:   []byte("short"),
		ContentLength: 5,
		ContentSummary: "short",
	}
	modifyDoc := pin.PinInscription{
		Id:            "modifyPinId",
		Address:       "addr-1",
		ContentBody:   []byte("longer-body"),
		ContentLength: 11,
	}
	if err := idx.BatchInsertPins([]pin.PinInscription{create, modifyDoc}); err != nil {
		t.Fatalf("BatchInsertPins err: %v", err)
	}

	err = idx.BatchUpdatePins([]*pin.PinInscription{{
		Id:            "modifyPinId",
		OriginalId:    "createPinId",
		Address:       "addr-1",
		Status:        1,
		ContentBody:   []byte("longer-body"),
		ContentLength: 11,
		ContentSummary: "longer-body",
	}})
	if err != nil {
		t.Fatalf("BatchUpdatePins err: %v", err)
	}

	got, err := idx.GetPinInscriptionByKey("createPinId")
	if err != nil {
		t.Fatalf("GetPinInscriptionByKey err: %v", err)
	}
	if string(got.ContentBody) != "longer-body" {
		t.Fatalf("original ContentBody = %q, want longer-body", got.ContentBody)
	}
	if got.ContentLength != 11 {
		t.Fatalf("original ContentLength = %d, want 11", got.ContentLength)
	}
	if got.Status != 1 {
		t.Fatalf("original Status = %d, want 1", got.Status)
	}

	kept, err := idx.GetPinInscriptionByKey("modifyPinId")
	if err != nil {
		t.Fatalf("modify pin lookup err: %v", err)
	}
	if string(kept.ContentBody) != "longer-body" {
		t.Fatalf("modify pin should stay inserted, body = %q", kept.ContentBody)
	}
	if kept.Status != 0 {
		t.Fatalf("modify pin status = %d, want 0", kept.Status)
	}
}
