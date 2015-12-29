package com.lilith.leveldb.impl;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class ParsedInternalKey {
  public Slice user_key = null;
  public long sequence = 0;
  public byte type = 0;
  
  public ParsedInternalKey() {
    
  }
  
  public ParsedInternalKey(Slice u, long seq, byte type) {
    this.sequence = seq;
    this.user_key = u;
    this.type = type;
  }
  
  public long InternalKeyEncodingLength() {
    return user_key.GetLength() + 8;        
  }
  
  
  public ParsedInternalKey ParseInternalKey(Slice internal_key) {
    int n = internal_key.GetLength();
    if (n < 8) return null;
    long num = BinaryUtil.DecodeVarint64(internal_key.GetData(), internal_key.GetOffset() + n - Settings.UINT64_SIZE);
    byte c = (byte) (num & 0xff);
    
    Slice user_key = new Slice(internal_key.GetData(), internal_key.GetOffset(), internal_key.GetLength() - Settings.UINT64_SIZE);
    ParsedInternalKey result = new ParsedInternalKey(user_key, num >> 8, c);
    return result;
  }
}