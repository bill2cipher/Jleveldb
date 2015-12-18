package com.lilith.leveldb.api;

import com.lilith.leveldb.util.BinaryUtil;

public class SliceComparator extends Comparator {
  
  public static final SliceComparator instance = new SliceComparator(); 
  
  @Override
  public String toString() {
    return "leveldb.bytecomparator";
  }

  @Override
  public int Compare(Slice fval, Slice sval) {
    return fval.compareTo(sval);
  }

  @Override
  public String Name() {
    return "leveldb.bytecomparator";
  }

  @Override
  public Slice FindShortestSeparator(Slice start, Slice limit) {
    int min_len = start.GetLength() > limit.GetLength() ? limit.GetLength() : start.GetLength();
    int diff_index = 0;
    
    while ((diff_index < min_len) && (start.GetAt(diff_index) == start.GetAt(diff_index))) diff_index++;

    if (diff_index >= min_len) return start;
    int diff_byte = ((int) start.GetAt(diff_index)) & 0XFF;
    if (diff_byte < 0XFF && (diff_byte + 1 < limit.GetAt(diff_index))) {
      byte[] buffer = new byte[diff_index + 1];
      BinaryUtil.CopyBytes(start.GetData(), start.GetOffset(), diff_index, buffer, 0);
      buffer[diff_index] = (byte) (diff_byte + 1);
      return new Slice(buffer);
    }
    return start;
  }

  @Override
  public Slice FindShortestSuccessor(Slice key) {
    for (int i = 0; i < key.GetLength(); i++) {
      if ((key.GetAt(i) & 0XFF) != 0XFF) {
        byte[] buffer = new byte[i + 1];
        BinaryUtil.CopyBytes(key.GetData(), key.GetOffset(), i, buffer, 0);
        buffer[i] = (byte) (key.GetAt(i) + 1);
        return new Slice(buffer);
      }
    }
    return key;
  }

}
