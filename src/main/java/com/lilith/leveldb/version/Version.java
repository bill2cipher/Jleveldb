package com.lilith.leveldb.version;

import java.util.ArrayList;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.Settings;

public class Version {
  
  // List of files per level.
  private ArrayList<FileMetaData>[] files = null;
  
  private VersionSet vset = null;
  private Version next = null;
  private Version prev = null;
  
  // level should be compacted next and its compaction score. Score < 1 means
  // compaction is not strictly needed.
  int compaction_score = 0;
  int compaction_level = 0;
  
  // next file to compact based on seek states
  private FileMetaData file_to_compact = null;
  private int file_to_compact_level = 0;
  
  public Version(VersionSet vset) {
    this.vset = vset;
    this.next = this;
    this.prev = this;
    this.file_to_compact = null;
    this.file_to_compact_level = -1;
    this.compaction_level = -1;
    this.compaction_score = -1;
    
    this.files = (ArrayList<FileMetaData>[]) new Object[Settings.NUM_LEVELS];
    for (int i = 0; i < files.length; i++) files[i] = new ArrayList<FileMetaData>();
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
