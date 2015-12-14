package com.lilith.leveldb.table;

import com.lilith.leveldb.api.Slice;

public class ShardedLRUCache implements Cache {
  
  private final static int NumShardBits = 4;
  private final static int NumShards = 1 << NumShardBits;    

  public void Insert(Slice key, Slice value, int charge) {
    // TODO Auto-generated method stub
    
  }

  public Slice Lookup(Slice key) {
    // TODO Auto-generated method stub
    return null;
  }

  public void Erase(Slice key) {
    // TODO Auto-generated method stub
    
  }

  public long NewId() {
    // TODO Auto-generated method stub
    return 0;
  }

}
