package com.lilith.leveldb.table;

import java.util.ArrayList;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

/**
 * A FilterBlockBuilder is used to construct all of the filters for a particular Table.
 * It generates a single byte[] list which is stored as a special block in the Table.
 * 
 * The sequence of calls to FilterBlockBuilder must match the 
 */
public class FilterBlockBuilder {
  
  private FilterPolicy policy;
  private ArrayList<Slice> keys;
  private ArrayList<Integer> start;
  private ArrayList<Integer> filter_offsets;
  private ArrayList<Slice> result;
  private ArrayList<Slice> tmp_keys;
  
  private final static int FilterBaseLg = 11;
  private final static int FilterBase = 1 << FilterBaseLg;
  
  
  public FilterBlockBuilder(FilterPolicy policy) {
    this.policy = policy;
    this.start = new ArrayList<Integer>();
    this.filter_offsets = new ArrayList<Integer>();
    this.tmp_keys = new ArrayList<Slice>();
    this.result = new ArrayList<Slice>();
    this.keys = new ArrayList<Slice>();
  }
  
  public void StartBlock(int block_offset) {
    int filter_index = block_offset / FilterBase;
    while (filter_index > filter_offsets.size())
      GenerateFilter();
  }
  
  public void AddKey(Slice key) {
    start.add(key.GetLength());
    keys.add(key);
  }
  
  public Slice Finish() {
    if (!start.isEmpty()) GenerateFilter();
    
    final int array_offset = result.size();
    int offset = 0;
    byte[] buffer = new byte[(filter_offsets.size() + 2) * Settings.UINT32_SIZE];
    for (int i = 0; i < filter_offsets.size(); i++) {
      BinaryUtil.PutVarint32(buffer, offset, filter_offsets.get(i));
      offset += Settings.UINT32_SIZE;
    }
    
    BinaryUtil.PutVarint32(buffer, offset, array_offset); offset += Settings.UINT32_SIZE;
    BinaryUtil.PutVarint32(buffer, offset, FilterBaseLg);
    return new Slice(buffer);
  }
  
  private void GenerateFilter() {
    final int num_keys = start.size();
    if (num_keys == 0) {
      filter_offsets.add(result.size());
      return;
    }
    // Make list of keys from flattened key structure.
    start.add(keys.size());
    for (int i = 0; i < num_keys; i++) {
      
    }
    
    filter_offsets.add(result.size());
    Slice result = policy.CreateFilter(tmp_keys);
  }
}
