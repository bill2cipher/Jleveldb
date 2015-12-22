package com.lilith.leveldb.table;

import java.util.ArrayList;
import java.util.Iterator;

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
  private ArrayList<Integer> filter_offsets;
  private ArrayList<Slice> result;
  private int cur_offset = 0;
  
  private final static int FilterBaseLg = 11;
  private final static int FilterBase = 1 << FilterBaseLg;
  
  
  public FilterBlockBuilder(FilterPolicy policy) {
    this.policy = policy;
    this.filter_offsets = new ArrayList<Integer>();
    this.result = new ArrayList<Slice>();
    this.keys = new ArrayList<Slice>();
    this.cur_offset = 0;
  }
  
  public void StartBlock(int block_offset) {
    int filter_index = block_offset / FilterBase;
    while (filter_index > filter_offsets.size()) {
      GenerateFilter();
    }
  }
  
  public void AddKey(Slice key) {
    keys.add(key);
  }
  
  public Slice Finish() {
    if (!keys.isEmpty()) GenerateFilter();
    int offset = 0;
    byte[] buffer = new byte[cur_offset + (filter_offsets.size() + 1) * Settings.UINT32_SIZE + 1];
    
    Iterator<Slice> iter = result.iterator();
    while (iter.hasNext()) {
      Slice filter = iter.next();
      BinaryUtil.CopyBytes(filter.GetData(), filter.GetOffset(), filter.GetLength(), buffer, offset);
      offset += filter.GetLength();
    }

    for (int i = 0; i < filter_offsets.size(); i++) {
      BinaryUtil.PutVarint32(buffer, offset, filter_offsets.get(i));
      offset += Settings.UINT32_SIZE;
    }
    BinaryUtil.PutVarint32(buffer, offset, cur_offset); offset += Settings.UINT32_SIZE;
    buffer[offset] = (byte) FilterBase;
    return new Slice(buffer);
  }
  
  private void GenerateFilter() {
    final int num_keys = keys.size();
    if (num_keys == 0) {
      filter_offsets.add(cur_offset);
      return;
    }

    filter_offsets.add(cur_offset);
    
    Slice filter = policy.CreateFilter(keys);
    cur_offset += filter.GetLength();

    result.add(filter);
    keys.clear();
  }
}
