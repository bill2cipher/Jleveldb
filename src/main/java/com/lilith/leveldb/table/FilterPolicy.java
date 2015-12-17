package com.lilith.leveldb.table;

import com.lilith.leveldb.util.Settings;

public abstract class FilterPolicy {
  
  /**
   * Get the filter policy instance according to the given filter_type
   */
  public static FilterPolicy GetFilterPolicy(int filter_type) {
    switch (filter_type) {
    case Settings.NO_FILTER_POLICY:
      return null;
    case Settings.BLOOM_FILTER_POLICY:
      return BloomFilterPolicy.GetInstance();
    default:
      return null;
    }
  }
  
    
}
