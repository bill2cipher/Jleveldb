package com.lilith.leveldb.table;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class FilterBlockReader {
  
  private FilterPolicy policy = null;
  private Slice data = Slice.EmptySlice;   // contents of filter data
  private int offset = 0;                  // the beginning of the offset array
  private int num = 0;                     // num of entries in the offset array
  private int base_lg = 0;                 // encoding parameter
  
  
  public FilterBlockReader(FilterPolicy policy, Slice contents) {
    int len = contents.GetLength();
    this.base_lg = contents.GetAt(len) & 0XFF;
    
    this.policy = policy;
    
    this.offset = BinaryUtil.DecodeVarint32(contents.GetData(), len - 1 - Settings.UINT32_SIZE);
    this.num = (len - 1 - Settings.UINT32_SIZE - offset) / Settings.UINT32_SIZE;
    
    this.data = contents;    
  }
  
  public boolean KeyMayMatch(int block_offset, Slice key) {
    int filter_index = block_offset >> base_lg;
    int index_offset = offset + filter_index * Settings.UINT32_SIZE;
    int data_start = BinaryUtil.DecodeVarint32(data.GetData(), data.GetOffset() + index_offset);
    int data_limit = BinaryUtil.DecodeVarint32(data.GetData(), data.GetOffset() + index_offset + Settings.UINT32_SIZE);
    if (data_start <= data_limit && data_limit <= offset) {
      Slice filter = new Slice(data.GetData(), data_start, data_limit - data_start);
      return policy.KeyMayMatch(key, filter);
    } else if (data_start == data_limit) {
      return false;
    }
    return true;
  }
}
