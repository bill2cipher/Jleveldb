package com.lilith.leveldb.impl;

import java.io.DataOutputStream;
import java.util.ArrayList;

import com.lilith.leveldb.table.TableBuilder;
import com.lilith.leveldb.version.Compaction;
import com.lilith.leveldb.version.InternalKey;

public class CompactionState {
  
  public static class Output {
    public long number = 0;
    public int file_size = 0;
    InternalKey smallest;
    InternalKey largest;
  }
  
  public Compaction compaction = null;
  
  public long smallest_snapshot = 0;
  
  public ArrayList<Output> outputs = null;
  
  public DataOutputStream outfile = null;
  
  public TableBuilder builder = null;
  
  public long total_bytes = 0;
  
  public Output current_output() {
    return outputs.get(outputs.size() - 1);
  }
  
  public CompactionState(Compaction c) {
    compaction = c;
    outfile = null;
    builder = null;
    total_bytes = 0;
    outputs = new ArrayList<Output>();
  }
}
