package main

import (
	"fmt"
	"log"

	"github.com/boltdb/bolt"
)

func main() {
	fmt.Println("hello fys, welcome to bolt db")

	write_kv([]byte("table_1"), []byte("name"), []byte("fys"))

	v, err := read_kv([]byte("table_1"), []byte("name"))

	if err != nil {
		log.Fatal(err)
	}

	log.Println(v)
}

func write_kv(table []byte, key []byte, value []byte) {
	db, err := bolt.Open("fys.db", 0600, nil)

	if err != nil {
		log.Fatal(err)
		return
	}

	defer db.Close()

	// 参数true表示创建一个写事务，false读事务
	tx, err := db.Begin(true)
	if err != nil {
		log.Fatal(err)
		return
	}

	defer tx.Rollback()

	b, err := tx.CreateBucketIfNotExists(table)
	if err != nil {
		log.Fatal(err)
		return
	}
	// 使用bucket对象更新一个key
	if err := b.Put(key, value); err != nil {
		log.Fatal(err)
		return
	}
	// 提交事务
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
		return
	}
}

func read_kv(table []byte, key []byte) ([]byte, error) {
	db, err := bolt.Open("fys.db", 0600, nil)

	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	defer db.Close()

	var val []byte
	db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(table)
		val = bucket.Get(key)
		return nil
	})
	return val, nil

}
