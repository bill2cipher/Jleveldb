package com.lilith.leveldb.util;

import com.lilith.leveldb.api.Slice;

public class Range {
  public Slice start = null;
  public Slice limit = null;
  
  public Range() {
    
  }
  
  public Range(Slice s, Slice l) {
    start = s;
    limit = l;
  }
}
