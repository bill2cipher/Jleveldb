package com.lilith.leveldb;

import com.lilith.leveldb.api.Slice;

public class Util {
  public static Slice str2slice(String val) {
    return new Slice(val.getBytes());
  }
  
  public static String slice2str(Slice val) {
    return new String(val.GetData(), val.GetOffset(), val.GetLength());
  }
}
