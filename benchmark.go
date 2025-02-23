package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	BTREE_DB     = "btree"
	IN_MEMORY_DB = "in-memory"
	ALL          = "all"
)

func stressTest(db KVStore, numOps, numWokers int) {
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < numWokers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				// add a key
				key := fmt.Sprintf("key-%d-%d", worker, j)
				value := []byte(fmt.Sprintf("value-%d-%d", worker, j))
				if err := db.Put(key, value); err != nil {
					log.Fatalf("worker %d: put failed: %s", worker, err)
				}
				// get the key
				val, err := db.Get(key)
				if err != nil || val == nil {
					log.Fatalf("worker %d: get failed: %s", worker, err)
				}

				if string(val) != string(value) {
					log.Fatalf("worker %d: get failed: expected %s, got %s", worker, value, val)
				}

				// delete the key
				if err := db.Delete(key); err != nil {
					log.Fatalf("worker %d: delete failed: %s", worker, err)
				}

				// get the key again
				val, err = db.Get(key)
				if err != nil || val != nil {
					log.Fatalf("worker %d: get failed: %s", worker, err)
				}
			}
			log.Printf("test passed for worker %d", worker)
		}(i)

	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("numOps: %d, numWorkers: %d, timeElapsed: %s\n", numOps*numWokers, numWokers, elapsed)
}

func main() {

	// Define CLI flags for workers and operations
	numWorkersFlag := flag.Int("workers", 10, "Number of concurrent workers")
	numOpsFlag := flag.Int("ops", 100, "Number of operations per worker")
	storageTypeFlag := flag.String("storage", "btree", "Storage type: btree, in-memory or all")

	flag.Parse()

	numWorkers := *numWorkersFlag
	numOps := *numOpsFlag

	storageType := *storageTypeFlag

	if (storageType != BTREE_DB) && (storageType != IN_MEMORY_DB) && (storageType != ALL) {
		log.Fatalf("Invalid storage type: %s", storageType)
	}

	dbs := map[string]KVStore{}

	if storageType == ALL || storageType == BTREE_DB {
		btreeDB, err := NewBoltBTreeKV()
		if err != nil {
			log.Fatal(fmt.Errorf("failed to create boltbtree db: %s", err))
		}
		dbs[BTREE_DB] = btreeDB
	}

	if storageType == "all" || storageType == IN_MEMORY_DB {
		inMemoryDB := NewInMemoryKV()
		dbs[IN_MEMORY_DB] = inMemoryDB
	}
	for dbType, dbToTest := range dbs {
		log.Printf("running stress test for %s with %d workers, %d ops", dbType, numWorkers, numOps)
		stressTest(dbToTest, numOps, numWorkers)
	}
}
