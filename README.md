# Overview

This repo is for exploring different KV stores in Go and understanding their design choices, while also building a simple KV store in the process.

# Design choices

- flat keyspaces (like badger, rocksdb)
- {more}

# How to run benchmark

- compile

```
go build -o main *.go
```

- Run

```
./main --workers 10 --ops 10 --storage btree
```

# Benchmark Results

## Inmemory
This is just a inmemory map that uses a lock to synchronize writes

- numOps: 100, numWorkers: 10, timeElapsed: 335.697µs

## boltBtreeDB

- numOps: 10, numWorkers = 10, timeElasped: 8s
- numOps: 100, numWorkers = 10, timeElasped: 1m22s

Pretty slow, explained by the fact that each write locks the whole DB. Ok, so why not lock just the key? its because of BTree structure, each insertion or update can change the tree structure.

