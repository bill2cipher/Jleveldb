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
  private SkipList<Slice, KeyComparator> Table;
  private Table table = null;
  
  public MemTable(Options options) {
    buffer = new byte[options.block_size];
  }
  
  /** 、
   * Add an entry into memtble that maps key to value at the 
   * specified sequence number and with the specified type.
   * typically value will be empty if type ==  TypeDeletion.
   * @param sequence
   * @param op_type
   * @param key
   * @param value
   */
  public void Add(long sequence, int op_type, Slice key, Slice value) {
    
  }
  
  /**
   * return an estimate of the number of bytes data in use by this
   * data structure.
   * 
   * external synchronization to prevent simultaneous operation on the same table
   * @return
   */
  public int ApproximateMemoryUsage() {
    return 0;
  }
  
  /**
   * Return an iterator that yields the contents of the memtable.
   * the caller must ensure underlying memtable remains live while
   * the returned iterator is live. The keys returned by this iterator are internal
   * keys encoded by AppendInternalKey in the db/format.h module
   * @return
   */
  public MemIterator Iterator() {
    return null;
  }
}
