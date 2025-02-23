package main

import "sync"

type InMemoryKV struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewInMemoryKV() *InMemoryKV {
	return &InMemoryKV{data: make(map[string][]byte)}
}

func (i *InMemoryKV) Get(key string) ([]byte, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.data[key], nil
}

func (i *InMemoryKV) Put(key string, value []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.data[key] = value
	return nil
}

func (i *InMemoryKV) Delete(key string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.data, key)
	return nil
}

func (i *InMemoryKV) Close() error {
	return nil
}
