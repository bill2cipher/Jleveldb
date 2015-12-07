package com.lilith.leveldb.version;

import com.lilith.leveldb.api.Slice;

/**
 * A compaction encapsulates information about a compaction.
 *
 */
public class Compaction {
  private int level = 0;
  private VersionEdit edit = null;
  
  /**
   * Return the level that is being compacted. Inputs from "level" and 
   * "level + 1" will be merged to produce a set of "level + 1" files.
   */
  public int Level() {
    return level;
  }
  
  /**
   * Return the object that holds the edits to the descriptor done by
   * this compaction.
   */
  public VersionEdit Edit() {
    return edit;
  }
  
  /**
   * Which must be either 0 or 1.
   */
  public int NumInputFiles(int which) {
    return inputs[which].size();
  }
  
  /**
   * Return the ith input file at level() + which
   */
  public FileMetaData Input(int which, int i) {
    return inputs[which][i];
  }
  
  /**
   * Maximum size of files to build during this compaction
   */
  public long MaxOutputFileSize() {
    return max_output_file_size;
  }
  
  /**
   * Is this a trivial compaction that can be implemented by just 
   * moving a single input file to the next level.
   */
  public boolean IsTrivialMove() {
    return false;
  }
  
  /**
   * Add all inputs to this compaction as delete operations to edit
   */
  public boolean AddInputDeletions(VersionEdit edit) {
    return false;
  }
  
  /**
   * Return true if the information we have available guarantees that the
   * compaction is producing data in level + 1 for which no data exists
   * in levels greater than level + 1.
   */
  public boolean IsBaseLevelForKey(Slice key) {
    return false;
  }
  
  /**
   * Return true iff we should stop building the current output before
   * processing key
   */
  public boolean ShouldStopBefore(Slice key) {
    return false;
  }
  
  /**
   * Release the input version for the compaction, once the compaction is successful.
   */
  public void ReleaseInputs() {
    
  }
}
