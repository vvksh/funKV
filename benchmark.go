package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"
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
	fmt.Printf("ops: %d, workers: %d, elapsed: %s\n", numOps*numWokers, numWokers, elapsed)
}

func main() {

	// Define CLI flags for workers and operations
	numWorkersFlag := flag.Int("workers", 10, "Number of concurrent workers")
	numOpsFlag := flag.Int("ops", 100, "Number of operations per worker")
	storageType := flag.String("storage", "btree", "Storage type: btree or lsm")

	dbs := map[string]KVStore{}
	btreeDB, err := NewBoltBTreeKV()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to create boltbtree db: %s", err))
	}
	dbs["btree"] = btreeDB

	numWorkers := *numWorkersFlag
	numOps := *numOpsFlag

	dbToTest := dbs[*storageType]
	if dbToTest == nil {
		log.Fatalf("invalid storage type: %s", *storageType)
	}
	log.Printf("running stress test for %s with %d workers, %d ops", *storageType, numWorkers, numOps)
	stressTest(dbToTest, numOps, numWorkers)
}
