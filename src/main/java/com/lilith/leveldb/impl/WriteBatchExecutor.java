package com.lilith.leveldb.impl;

import com.lilith.leveldb.api.Slice;

public interface WriteBatchExecutor {
  
  public void Delete(Slice key);
  
  public void Put(Slice key, Slice value);
}
