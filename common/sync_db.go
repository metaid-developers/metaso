package common

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)

const bucketName = "SyncData"

var db *bolt.DB

var ErrSyncDBNotInitialized = errors.New("sync db not initialized")

// 初始化数据库
func InitSyncDB() (err error) {
	dbFile := Config.SyncDB
	if dbFile == "" {
		return nil // 如果没有配置数据库文件，则不初始化
	}
	db, err = bolt.Open(dbFile, 0600, nil)
	if err != nil {
		return err
	}

	// 创建桶
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	return err
}

// 保存数据
func SaveToDictDB(key string, value []byte) error {
	if db == nil {
		return ErrSyncDBNotInitialized
	}
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.Put([]byte(key), value)
	})
}

// 从数据库加载数据
func LoadFromDictDB(key string) ([]byte, error) {
	if db == nil {
		return nil, ErrSyncDBNotInitialized
	}
	var data []byte
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		value := b.Get([]byte(key))
		if value == nil {
			return nil // key 不存在
		}
		data = value
		return nil
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}
