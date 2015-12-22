package com.lilith.leveldb.table;


import com.lilith.leveldb.api.Comparator;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class BlockIterator {
  private Comparator cmp = null;
  private final byte[] data;
  private final int offset;
  private final int size;
  
  private int cur_offset = 0;
  private int restart_index = 0;
  private Slice last_key = null;
  private Slice last_value = null;
  
  private final int restart_offset;
  private final int num_entries;

  public BlockIterator(Comparator cmp, byte[] data, int offset, int size, int restart_offset, int num_entries) {
    this.cur_offset = restart_offset;
    this.restart_index = num_entries;
    this.last_key = Slice.EmptySlice;
    this.last_value = Slice.EmptySlice;
    
    this.num_entries = num_entries;
    this.restart_offset = restart_offset;
    
    this.data = data;
    this.offset = offset;
    this.size = size;
    
    this.cmp = cmp;
  }
  
  public boolean Valid() {
    return cur_offset < restart_offset;
  }
  
  private int Compare(Slice fval, Slice sval) {
    return cmp.Compare(fval,  sval);        
  }
  
  private int GetRestartPoint(int index) {
    return BinaryUtil.DecodeVarint32(data, restart_offset + index * Settings.UINT32_SIZE);
  }
  
  private void SeekToRestartPoint(int index) {
    restart_index = index;
    cur_offset = GetRestartPoint(index);
    last_value = new Slice(data, cur_offset, 0);
    last_key = Slice.EmptySlice;
  }
  
  private int NextEntryOffset() {
    return last_value.GetOffset() + last_value.GetLength();
  }
  
  public Slice Key() {
    if (!Valid()) return null;
    return last_key;
  }
  
  public Slice Value() {
    if (!Valid()) return null;
    return last_value;
  }
  
  public boolean Next() throws BadFormatException {
    return ParseNextKey() != null;
  }
  
  public boolean Prev() throws BadFormatException {
    int original = cur_offset;

    while(GetRestartPoint(restart_index) >= original) {
      if (restart_index <= 0) return false;
      restart_index--;
    }
    
    SeekToRestartPoint(restart_index);
    while (true) {
      if (ParseNextKey() == null) return false;
      if (NextEntryOffset() < original) continue;
      break;
    }
    return true;
  }
  
  public void Seek(Slice target) throws BadFormatException {
    int left = 0, right = num_entries - 1;
    int mid = 0, region_offset = 0;
    int[] values = new int[3];
    
    while (left < right) {
      mid = (left + right + 1) / 2;
      region_offset = GetRestartPoint(mid);
      values = new int[3];
      int key_offset = Block.DecodeEntry(data, region_offset, values, size + offset);
      Slice cur_key = new Slice(data, key_offset, values[1]);
      if (Compare(cur_key, target) < 0) {
        left = mid;
      } else {
        right = mid - 1;
      }
    }
    
    SeekToRestartPoint(left);
    while (true) {
      if (ParseNextKey() == null) return;
      if (Compare(last_key, target) >= 0) return;
    } 
  }
  
  public void SeekToFirst() throws BadFormatException {
    SeekToRestartPoint(0);
    ParseNextKey();
  }
  
  public void SeekToLast() throws BadFormatException {
    SeekToRestartPoint(num_entries - 1);
    while (ParseNextKey() != null) {
      if (NextEntryOffset() < restart_offset) continue;
    }
  }

  public Slice ParseNextKey() throws BadFormatException {
    cur_offset = NextEntryOffset();
    if (cur_offset > offset + size) {
      cur_offset = restart_offset;
      restart_index = num_entries;
      return null;
    }
    int[] values = new int[3];
    int key_offset = Block.DecodeEntry(data, cur_offset, values, size + offset);
    if (key_offset == -1 || last_key.GetLength() < values[0]) return null;
    
    int shared = values[0], not_shared = values[1], value_size = values[2];
    byte[] entry = new byte[shared + not_shared];
    
    BinaryUtil.CopyBytes(last_key.GetData(), last_key.GetOffset(), shared, entry, 0);
    BinaryUtil.CopyBytes(data, key_offset, not_shared, entry, shared);
    last_key = new Slice(entry);
    
    last_value = new Slice(data, key_offset + not_shared, value_size);
    
    while (restart_index + 1 < num_entries) {
      if (GetRestartPoint(restart_index + 1) < cur_offset) restart_index++;
      else break;
    }
    return new Slice(entry);
  }
}
