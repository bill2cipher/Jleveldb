package com.lilith.leveldb.table;

import com.lilith.leveldb.api.Slice;

public class FilterBlockReader {
  
  private FilterPolicy policy = null;
  private byte[] data = new byte[0];   // contents of filter data
  private int offset = 0;              // the beginning of the data
  private int num = 0;                 // num of entries in the offset array
  private int base_lg = 0;             // encoding parameter
  
  
  public FilterBlockReader(FilterPolicy policy, Slice contents) {
    
  }
  
  public boolean KeyMayMatch(int block_offset, Slice key) {
    return false;
  }
}
