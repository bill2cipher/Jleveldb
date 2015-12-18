package com.lilith.leveldb.table;

import java.util.ArrayList;

import com.lilith.leveldb.api.Slice;
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
  
  /**
   * Return the name of this policy. Note that if the filter encoding
   * changes in an incompatible way, the name returned by this method
   * must be changed. Otherwise, old incompatible filters may be
   * passed to methods of this type.
   */
  public abstract Slice Name();
  
  /**
   * keys[0, n-1] contains a list of keys (potentially with duplicates)
   * that are ordered according to the user supplied comparator.
   * return a filter that summarizes keys[0, n-1].
   */
  public abstract Slice CreateFilter(ArrayList<Slice> keys);
}
