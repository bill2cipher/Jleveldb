package com.lilith.leveldb.table;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.Settings;

public class TableCache {
  
  private String dbname = null;
  private Options options = null;
  private Cache cache = null;
  
  public class Iterator {
    
  }
  
  public TableCache(String dbname, Options options, int entries) {
    this.dbname = dbname;
    this.options = options;
    this.cache = new LRUCache(entries);
  }
  
  /**
   * Return an iterator for the specified file number (the corresponding file length must be exactly 'file_size' bytes).
   */
  public Iterator TableCacheIterator(ReadOptions options, long file_num, int file_size) {
    return new Iterator();
  }
  
  /**
   * If a seek to internal key in specified file finds an entry, null if not found.
   */
  public Slice Get(ReadOptions options, long file_num, int file_size, Slice intern_key) {
    Table table = FindTable(file_num, file_size);
  }
  
  /**
   * Evict any entry for the specified file number
   */
  public void Evict(long file_num) {
   byte[] buffer = new byte[Settings.UINT64_SIZE];
   BinaryUtil.PutVarint64(buffer, 0, file_num);
   cache.Erase(new Slice(buffer));
  }
}
