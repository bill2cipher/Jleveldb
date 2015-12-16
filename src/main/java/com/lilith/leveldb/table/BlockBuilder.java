package com.lilith.leveldb.table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.Settings;

public class BlockBuilder {
  
  private ArrayList<byte[]> buffer;                // stores content of blocks
  private Options options = null;                  // option for user to configuration
  private Vector<Integer> restart_points = null;   // lists of restart points within this block
  private int counter = 0;                         // number of entries since last restart
  private boolean finished = false;                // flag indicate finish of building
  private Slice last_key = Slice.EmptySlice;       // the last of the block
  private int current_size = 0;                    // current size of the block

  public BlockBuilder(Options options) {
    this.options = options;
    this.counter = 0;
    this.finished = false;
    this.restart_points.add(0);
    this.last_key = Slice.EmptySlice;  
    current_size = 0;
    buffer = new ArrayList<byte[]>();
  }

  public void Reset() {
    buffer.clear();
    restart_points.clear();
    restart_points.add(0);
    counter = 0;
    finished = false;
    last_key = Slice.EmptySlice;
    current_size = 0;
  }

  /**
   * Append another key value pair into this block
   */
  public boolean Add(Slice key, Slice value) {
    if (counter > options.block_restart_interval) return false;
    if (this.finished) return false;
    
    int shared = 0;
    if(counter <= options.block_restart_interval) {
      int min_length = last_key.GetLength() > key.GetLength()? key.GetLength() : last_key.GetLength();
      while ((shared < min_length) && (last_key.GetAt(shared) == last_key.GetAt(shared))) shared ++;
    } else {
      restart_points.add(current_size);
      counter = 0;
    }
    
    int not_shared = key.GetLength() - shared;
    byte[] store = new byte[Settings.UINT32_SIZE * 3 + not_shared + value.GetLength()];  // shared + not_shared + value_size
    BinaryUtil.PutVarint32(store, 0, shared);
    BinaryUtil.PutVarint32(store, Settings.UINT32_SIZE, not_shared);
    BinaryUtil.PutVarint32(store, Settings.UINT32_SIZE * 2, value.GetLength());
    for (int i = shared, j = Settings.UINT32_SIZE * 3; i < key.GetLength(); i++, j++) {
      store[j] = key.GetAt(i);
    }
    for (int i = 0, j = Settings.UINT32_SIZE * 3 + not_shared ; i < value.GetLength(); i++, j++) {
      store[j] = value.GetAt(i);
    }
    counter++;
    current_size += Settings.UINT32_SIZE * 3 + not_shared + value.GetLength();
    this.last_key = key;
    return true;
  }
  
  /**
   * finish build the block and return the content of it.
   */
  public Slice Finish() {
    byte[] result = new byte[current_size + Settings.UINT32_SIZE * (restart_points.size() + 1)];
    int rslt_offset = 0;
    Iterator<byte[]> iter = buffer.iterator();
    while (iter.hasNext()) {
      byte[] data = iter.next();
      BinaryUtil.CopyBytes(data, 0, data.length, result, rslt_offset);
      rslt_offset += data.length;
    }

    // append restart points
    for (int i = 0; i < restart_points.size(); i++) {
      BinaryUtil.PutVarint32(result, rslt_offset, restart_points.get(i));
      rslt_offset += Settings.UINT32_SIZE;
    }
    
    BinaryUtil.PutVarint32(result, rslt_offset, restart_points.size());
    this.finished = true;
    return new Slice(result);
  }
  
  /**
   * Current size of the block.
   * @return
   */
  public int CurrentSizeEstimate() {
    return restart_points.size() * Settings.UINT32_SIZE + Settings.UINT32_SIZE + current_size;
  }
  
  public boolean Empty() {
    return buffer.isEmpty();
  }
}
