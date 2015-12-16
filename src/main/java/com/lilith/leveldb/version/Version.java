package com.lilith.leveldb.version;

import java.util.ArrayList;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.Settings;

public class Version {
  
  // List of files per level.
  ArrayList<FileMetaData>[] files = null;
  
  private VersionSet vset = null;
  Version next = null;
  Version prev = null;
  
  // level should be compacted next and its compaction score. Score < 1 means
  // compaction is not strictly needed.
  double compaction_score = 0;
  int compaction_level = 0;
  
  // next file to compact based on seek states
  FileMetaData file_to_compact = null;
  int file_to_compact_level = 0;
  
  public Version(VersionSet vset) {
    this.vset = vset;
    this.next = this;
    this.prev = this;

    this.file_to_compact = null;
    this.file_to_compact_level = -1;
    this.compaction_level = -1;
    this.compaction_score = -1;
    
    this.files = (ArrayList<FileMetaData>[]) new ArrayList[Settings.NUM_LEVELS];
    for (int i = 0; i < files.length; i++)
      files[i] = new ArrayList<FileMetaData>();
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
    return files[level].size();
  }
  
  /**
   * Return the level at which we should place a new memtable compaction result that
   * covers the range [smallest_key, largest_key].
   */
  public int PickLevelForMemTableOutput(Slice smallest_key, Slice largest_key) {
    return 0;
  }
  
  /**
   * For each file overlapping the given user_key, add it to the result list.
   */
  private ArrayList<FileMetaData> GetOverlappingFiles(Slice user_key, Slice internal_key) {
    return null;
  }
}
