package com.lilith.leveldb.table;

import java.util.Iterator;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class BlockIterator implements Iterator<Slice> {
  private int cur = 0;
  private int num_entries = 0;
  private int cur_offset = 0;
  private byte[] data = null;
  private byte[] last_key = null;
  
  
  public BlockIterator(byte[] data, int num_entries) {
    this.cur = 0;
    this.cur_offset = 0;
    this.num_entries = num_entries;
    this.data = data;
    this.last_key = new byte[0];
  }
  
  public boolean hasNext() {
    return cur < num_entries;
  }

  public Slice next() {
    int shared = BinaryUtil.DecodeVarint32(data, cur);
    int not_shared = BinaryUtil.DecodeVarint32(data, cur + Settings.UINT32_SIZE);
    int value_size = BinaryUtil.DecodeVarint32(data, cur + Settings.UINT32_SIZE * 2);
    byte[] entry = new byte[Settings.UINT32_SIZE * 2 + shared + not_shared + value_size];

    BinaryUtil.PutVarint32(entry, 0, not_shared + shared);
    BinaryUtil.PutVarint32(entry, Settings.UINT32_SIZE, value_size);
    BinaryUtil.CopyBytes(last_key, 0, shared, entry, Settings.UINT32_SIZE * 2);
    BinaryUtil.CopyBytes(data, cur_offset, value_size, entry, Settings.UINT32_SIZE * 2 + not_shared + shared);
    
    if (last_key.length < (shared + not_shared)) last_key = new byte[shared + not_shared];
    cur_offset += Settings.UINT32_SIZE * 3 + not_shared + value_size;
    cur++;

    return new Slice(entry);
  }
}
