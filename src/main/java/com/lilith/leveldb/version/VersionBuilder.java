package com.lilith.leveldb.version;

import java.util.ArrayList;
import java.util.HashSet;

import com.lilith.leveldb.util.Settings;

/**
 * A helper class so we can efficiently apply a whole sequence of edit to a particular state
 * without creating intermediate Version. 
 */
public class VersionBuilder {
  private VersionSet vset = null;
  private Version base = null;
  private LevelState[] levels = null;
  
  private class LevelState {
    ArrayList<Long> deleted_files;
    HashSet<FileMetaData> added_files = null;
  }
  
  public VersionBuilder(VersionSet vset, Version base) {
    this.vset = vset;
    this.base = base;
    this.levels = new LevelState[Settings.NUM_LEVELS];
    
    for (int level = 0; level < Settings.NUM_LEVELS; level++) {
      levels[level] = new LevelState();
      levels[level].added_files = new HashSet<FileMetaData>();
      levels[level].deleted_files = new ArrayList<Long>();
    }
  }
  
  public void Apply(VersionEdit edit) {
    // update compaction pointers
    for (int i = 0; i < edit.compact_pointers.size(); i++) {
      int level = edit.compact_pointers[i].first;
      vset.compact_pointer[level] = edit.compact_pointers[i].second.Encode().toString();
    }
    
    // Delete files
    
    // Add new files
  }
  
  public Version GetCurrentVersion() {
    return null;
  }
  
  private void MaybeAddFile(Version v, int level, FileMetaData file) {
    if (levels[level].deleted_files.contains(file)) {
      // File is deleted: do nothing
    } else {
      
    }
  }
}
