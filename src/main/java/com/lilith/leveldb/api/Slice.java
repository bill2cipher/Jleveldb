package com.lilith.leveldb.api;

import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class Slice implements Comparable<Slice> {
  private int length = 0;
  private byte[] data = null;
  private int offset = 0;
  
  public final static Slice EmptySlice = new Slice(new byte[0], 0, 0); 
  
  public Slice(int length) {
    this.length = length;
    this.data = new byte[length];
    this.offset = 0;
  }
  
  public Slice(byte[] data) {
    this.length = data.length;
    this.data = data;
    this.offset = 0;
  }
  
  public Slice(byte[] data, int length) {
    this.length = length;
    this.data = data;
    this.offset = 0;
  }
  
  public Slice(byte[] data, int offset, int length) {
    this.data = data;
    this.offset = offset;
    this.length = length;
  }
  
  public static Slice DecodeLengthPrefix(byte[] buffer, int offset) {
    int size = BinaryUtil.DecodeVarint32(buffer, offset);
    return new Slice(buffer, offset + Settings.UINT32_SIZE, size);
  }
  
  public static int EncodeLengthPrefix(byte[] buffer, int offset, Slice value) {
    BinaryUtil.PutVarint32(buffer, offset, value.GetLength());
    offset += Settings.UINT32_SIZE;
    BinaryUtil.CopyBytes(value.GetData(), value.GetOffset(), value.GetLength(), buffer, offset);
    return offset + value.GetLength();
  }
  
  public final byte GetAt(int index) {
    return data[index + offset];
  }
  
  public final int GetLength() {
    return this.length;
  }
  
  public final int GetOffset() {
    return this.offset;
  }
  
  public final byte[] GetData() {
    return data;
  }

  public int compareTo(Slice s) {
    return BinaryUtil.CompareBytes(data, offset, length, s.data, s.offset, s.length);
  }
}

