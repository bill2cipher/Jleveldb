package com.lilith.leveldb.util;

import java.util.Comparator;

public class LongComparator implements Comparator<Long> {

  public int compare(Long fval, Long sval) {
    return fval.compareTo(sval);
  }
}
