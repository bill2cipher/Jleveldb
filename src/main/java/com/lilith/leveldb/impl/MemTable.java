package com.lilith.leveldb.impl;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.SkipList;
import com.lilith.leveldb.table.Table;

/**
 * Storing key/value pairs in the memory.
 * @author Administrator
 *
 */
public class MemTable {
  private byte[] buffer = null;
  private SkipList<byte[], KeyComparator> Table;
  private Table table = null;
  private Arena arena = null;
  
  public MemTable(Options options) {
    buffer = new byte[options.block_size];
  }
  
  public void Add(long sequence, int op_type, Slice key, Slice value) {
    
  }
}
