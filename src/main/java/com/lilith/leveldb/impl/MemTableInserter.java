package com.lilith.leveldb.impl;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.Settings;

public class MemTableInserter implements WriteBatchExecutor {

  private MemTable mem_table;  // the memtable to operate on
  private long sequence;       // the sequence number of the execution of this batch
  
  public MemTableInserter(long sequence, MemTable mem_table) {
    this.mem_table = mem_table;
    this.sequence = sequence;
  }
  
  /**
   * Delete the specific key/value pair from database associate with the given memtable
   */
  public void Delete(Slice key) {
    mem_table.Add(sequence, Settings.OP_TYPE_DELETE, key, Slice.EmptySlice);
  }

  /**
   * Append a new key/value pair into the database associate with the given memtable
   */
  public void Put(Slice key, Slice value) {
    mem_table.Add(sequence, Settings.OP_TYPE_VALUE, key, value);    
  }

}
