package com.lilith.leveldb.version;

import java.util.ArrayList;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.ReadOptions;

public class Version {
  
  private ArrayList<ArrayList<FileMetaData>> files = null;
  
  private VersionSet vset = null;
  private Version next = null;
  private Version prev = null;
  
  public Version(VersionSet vset) {
    this.vset = vset;
    this.next = this;
    this.prev = this;
  }
  
  /**
   * Lookup the value for key. If found, return a slice representing
   * the value, else return null.
   */
  public Slice Get(ReadOptions options, Slice key) {
    return null;
  }
  
  /**
   * Number of files at the specified level.
   */
  public int NumFiles(int level) {
    return files.get(level).size();
  }
  
  /**
   * Return the level at which we should place a new memtable compaction result that
   * covers the range [smallest_key, largest_key].
   */
  private int PickLevelForMemTableOutput(Slice smallest_key, Slice largest_key) {
    return 0;
  }
}
