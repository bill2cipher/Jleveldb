package com.lilith.leveldb.version;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.Settings;

import java.util.ArrayList;
import java.util.List;

/**
 * A compaction encapsulates information about a compaction.
 *
 */
public class Compaction {
  private int level = 0;

  private VersionEdit edit = null;
  private Version input_version = null;

  private long max_output_file_size = 0;
  private List<FileMetaData>[] inputs = null; // Each compaction reads inputs from
                                            // level and level + 1
  private List<FileMetaData> grandparents = null; // State used to check for
                                                  // number of overlapping
                                                  // grandparent files

  private int grandparent_index = 0;
  private boolean seen_key = false;
  private long overlapping_bytes = 0;

  private int[] level_ptrs = null;

  public Compaction(int level) {
    this.level = level;
    this.max_output_file_size = VersionSet.MaxFileSizeForLevel(level);
    this.input_version = null;
    this.grandparent_index = 0;
    this.seen_key = false;
    this.overlapping_bytes = 0;
    this.level_ptrs = new int[Settings.NUM_LEVELS];
    
    this.inputs = (List<FileMetaData>[]) new Object[2];
    this.grandparents = new ArrayList<FileMetaData>();
    
    for (int i = 0; i < Settings.NUM_LEVELS; i++) {
      level_ptrs[i] = 0;
    }
  }

  /**
   * Return the level that is being compacted. Inputs from "level" and
   * "level + 1" will be merged to produce a set of "level + 1" files.
   */
  public int Level() {
    return level;
  }

  /**
   * Return the object that holds the edits to the descriptor done by this
   * compaction.
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
    return inputs[which].get(i);
  }

  /**
   * Maximum size of files to build during this compaction
   */
  public long MaxOutputFileSize() {
    return max_output_file_size;
  }

  /**
   * Is this a trivial compaction that can be implemented by just moving a
   * single input file to the next level.
   */
  public boolean IsTrivialMove() {
    return NumInputFiles(0) == 1 &&
           NumInputFiles(1) == 0 &&
           VersionSet.TotalFileSize(grandparents) <= VersionSet.MAX_GRANDPARENT_OVERLAY_BYTES;
  }

  /**
   * Add all inputs to this compaction as delete operations to edit
   */
  public boolean AddInputDeletions(VersionEdit edit) {
    return false;
  }

  /**
   * Return true if the information we have available guarantees that the
   * compaction is producing data in level + 1 for which no data exists in
   * levels greater than level + 1.
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
   * Release the input version for the compaction, once the compaction is
   * successful.
   */
  public void ReleaseInputs() {

  }
}
