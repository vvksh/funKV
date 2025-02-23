package main

import (
	"fmt"

	"github.com/boltdb/bolt"
)

const (
	BUCKET_NAME = "kv"
)

// BoltBTreeKV is a key-value store using BoltDB and B-Tree
type BoltBTreeKV struct {
	db *bolt.DB
}

func NewBoltBTreeKV() (*BoltBTreeKV, error) {
	// just use current directory for simplicity
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		return nil, err
	}
	// create the main bucket which will store all the keys
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BUCKET_NAME))
		if err != nil {
			return fmt.Errorf("error creating bucket: %s", err)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	// defer db.Close()
	return &BoltBTreeKV{db: db}, nil
}

func (b *BoltBTreeKV) Get(key string) ([]byte, error) {
	var value []byte
	// start a read-only transaction
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(BUCKET_NAME))
		value = bucket.Get([]byte(key))
		return nil
	})
	return value, err
}

func (b *BoltBTreeKV) Put(key string, value []byte) error {
	// start a writable transaction, get the bucket and set the value
	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(BUCKET_NAME))
		err := bucket.Put([]byte(key), value)
		return err
	})
	return err
}

func (b *BoltBTreeKV) Delete(key string) error {
	// start a writable transaction, get the bucket and delete the key
	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(BUCKET_NAME))
		err := bucket.Delete([]byte(key))
		return err
	})
	return err
}

func (b *BoltBTreeKV) Close() error {
	return b.db.Close()
}
