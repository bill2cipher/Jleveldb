package com.lilith.leveldb.table;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.exceptions.DecodeFailedException;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.FileName;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.Settings;

public class TableCache {
  
  private String dbname = null;
  private Options options = null;
  private Cache<Slice, Table> cache = null;

  public TableCache(String dbname, Options options, int entries) {
    this.dbname = dbname;
    this.options = options;
    this.cache = new ShardedLRUCache<Slice, Table>(entries);
  }

  /**
   * If a seek to internal key in specified file finds an entry, null if not
   * found.
   * @throws DecodeFailedException 
   * @throws IOException 
   * @throws BadFormatException 
   */
  public Table Get(long file_num, int file_size) throws IOException, DecodeFailedException, BadFormatException {
    return FindTable(file_num, file_size);
  }
  
  /**
   * Evict any entry for the specified file number
   */
  public void Evict(long file_num) {
    byte[] buffer = new byte[Settings.UINT64_SIZE];
    BinaryUtil.PutVarint64(buffer, 0, file_num);
    Slice key = new Slice(buffer);
    cache.Erase(key, key.hashCode());
  }

  private Table FindTable(long file_num, int file_size) throws IOException, DecodeFailedException, BadFormatException {
    byte[] buffer = new byte[Settings.UINT64_SIZE];
    BinaryUtil.PutVarint64(buffer, 0, file_num);
    Slice key = new Slice(buffer);
    Table value = cache.Lookup(key, key.hashCode());
    if (value == null) {
      String table_name = FileName.LogFileName(dbname, file_num);
      DataInputStream reader = new DataInputStream(new FileInputStream(table_name));
      value = Table.Open(reader, file_size, options);
      cache.Insert(key, value, 1, key.hashCode());
    }
    return value;
  }
}
