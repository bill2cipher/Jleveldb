package com.lilith.leveldb.version;

import java.util.Comparator;

public class InternalKeyComparator implements Comparator<InternalKey> {

  public int compare(InternalKey fkey, InternalKey skey) {
    
  }
  
  public String ComparatorName() {
    return "leveldb.InternalKeyComparator";
  }

}
