package com.lilith.leveldb.version;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class InternalKey {
  private long seq = 0;
  private byte op_type = 0;
  private Slice key = null;

  private byte[] rep = null;
  private boolean updated = true;
  
  /**
   * Extract the user key portion out of the internal key
   */
  public static Slice ExtractUserKey(Slice internal_key) {
    return new Slice(internal_key.GetData(), internal_key.GetOffset()
                   , internal_key.GetLength() - Settings.UINT64_SIZE);
  }
  
  public InternalKey() {
    this.seq = 0;
    this.op_type = 0;
    this.key = null;
    this.rep = null;
    this.updated = true;
  }
  
  public InternalKey(Slice key, long seq, byte op_type) {
    this.seq = seq;
    this.key = key;
    this.op_type = op_type;
    this.updated = true;
  }
  
  public void DecodeFrom(byte[] rep) {
    this.rep = rep;
    this.key = new Slice(rep, 0, rep.length - Settings.UINT32_SIZE);
    long seq_type = BinaryUtil.DecodeVarint64(rep, key.GetLength());
    this.op_type = (byte) (seq_type & 0XFF);
    this.seq = seq_type >> 8;        
  }
  
  public void DecodeFrom(byte[] data, int offset, int size) {
    byte[] store = new byte[size];
    BinaryUtil.CopyBytes(data, offset, size, store, 0);
    DecodeFrom(store);
  }
  
  public Slice Encode() {
    if (updated == false) return new Slice(rep);
    rep = new byte[Settings.UINT64_SIZE + key.GetLength()];
    BinaryUtil.CopyBytes(key.GetData(), key.GetOffset(), key.GetLength(), rep, 0);
    BinaryUtil.PutVarint64(rep, key.GetLength(), (seq << 8) & op_type);
    this.updated = false;
    return new Slice(rep);
  }
  
  public int GetInternalKeySize() {
    return Encode().GetLength();
  }
  
  public Slice GetUserKey() {
    return key;
  }

}
