package com.lilith.leveldb.table;

import java.util.concurrent.atomic.AtomicLong;

import com.lilith.leveldb.api.Slice;

public class ShardedLRUCache<Key extends Comparable<Key>, Value> implements Cache<Key, Value> {
  
  private final static int NumShardBits = 4;
  private final static int NumShards = 1 << NumShardBits;
  private LRUCache<Key, Value>[] shard = null;
  private AtomicLong cache_id = new AtomicLong(0);
  
  public ShardedLRUCache(int capacity) {
    cache_id.addAndGet(1);
    shard = (LRUCache<Key, Value>[]) new Object[NumShards];
    int per_shard = (capacity + (NumShards - 1) / NumShards);
    
    for (int i = 0; i < NumShards; i++) {
      shard[i] = new LRUCache<Key, Value>();
      shard[i].SetCapacity(per_shard);
    }
  }
  
  public void Insert(Key key, Value value, int charge, int hash) {
    shard[Shard(hash)].Insert(key, value, charge, hash);   
  }
  public Value Lookup(Key key, int hash) {
    return shard[Shard(hash)].Lookup(key, hash);
  }
  public void Erase(Key key, int hash) {
    shard[Shard(hash)].Erase(key, hash);
  }

  public long NewId() {
    return cache_id.addAndGet(1);
  }
  
  private int Shard(int hash) {
    return hash >> (32 - NumShardBits);
  }
}
