package shardstore

import "sync"

// keyedMutex hands out one lock per key, serializing the manifest
// read-modify-write cycle (and shard verification/repair) for each object
// while different objects proceed independently.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

type keyLock struct {
	sync.Mutex
	ref int
}

func (k *keyedMutex) lock(key string) func() {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = map[string]*keyLock{}
	}
	kl, ok := k.locks[key]
	if !ok {
		kl = &keyLock{}
		k.locks[key] = kl
	}
	kl.ref++
	k.mu.Unlock()

	kl.Lock()
	return func() {
		kl.Unlock()
		k.mu.Lock()
		kl.ref--
		if kl.ref == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
