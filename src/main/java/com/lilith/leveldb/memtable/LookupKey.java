package com.lilith.leveldb.memtable;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

/**
 * Helper class used to offer various kinds of key type.
 */
public class LookupKey {
  private byte[] data = null;

  // Initialize this for looking up user_key at a snapshot with the specified sequence number.
  public LookupKey(Slice user_key, long seq) {
    data = new byte[user_key.GetLength() + Settings.UINT64_SIZE + Settings.UINT32_SIZE];
    BinaryUtil.PutVarint32(data, 0, user_key.GetLength() + Settings.UINT64_SIZE);
    BinaryUtil.CopyBytes(user_key.GetData(), user_key.GetOffset(), user_key.GetLength(), data, Settings.UINT32_SIZE);
    BinaryUtil.PutVarint64(data, user_key.GetLength() + Settings.UINT32_SIZE, (seq << 8) & Settings.OP_TYPE_SEEK);
  }
  
  public Slice MemTableKey() {
    return new Slice(data, 0, data.length);
  }
  
  public Slice InternalKey() {
    return new Slice(data, Settings.UINT32_SIZE, data.length - Settings.UINT32_SIZE);
  }
  
  public Slice UserKey() {
    return new Slice(data, Settings.UINT32_SIZE, data.length - Settings.UINT32_SIZE + Settings.UINT64_SIZE);
  }
}
