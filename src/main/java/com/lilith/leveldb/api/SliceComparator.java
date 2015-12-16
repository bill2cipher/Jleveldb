package com.lilith.leveldb.api;

import java.util.Comparator;

public class SliceComparator implements Comparator<Slice> {

  public int compare(Slice fval, Slice sval) {
    return fval.compareTo(sval);
  }
  
  @Override
  public String toString() {
    return "leveldb.bytecomparator";
  }

}
