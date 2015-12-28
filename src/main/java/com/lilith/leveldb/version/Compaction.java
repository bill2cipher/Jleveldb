package com.lilith.leveldb.version;

import java.util.ArrayList;

import com.lilith.leveldb.api.Comparator;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.Settings;
import com.lilith.leveldb.util.Util;

/**
 * A compaction encapsulates information about a compaction.
 *
 */
public class Compaction {
  private int level = 0;

  private VersionEdit edit = null;
  Version input_version = null;

  private long max_output_file_size = 0;
  ArrayList<FileMetaData>[] inputs = null; // Each compaction reads inputs from
                                            // level and level + 1
  ArrayList<FileMetaData> grandparents = null; // State used to check for
                                                  // number of overlapping
                                                  // grandparent files

  private int grandparent_index = 0;
  private boolean seen_key = false;
  private long overlapping_bytes = 0;

  private int[] level_ptrs = null;

  public Compaction(int level) {
    this.level = level;
    this.max_output_file_size = VersionUtil.MaxFileSizeForLevel(level);
    this.input_version = null;
    this.grandparent_index = 0;
    this.seen_key = false;
    this.overlapping_bytes = 0;
    this.level_ptrs = new int[Settings.NUM_LEVELS];
    
    this.inputs = (ArrayList<FileMetaData>[]) new ArrayList[2];
    this.grandparents = new ArrayList<FileMetaData>();
    
    for (int i = 0; i < Settings.NUM_LEVELS; i++) {
      level_ptrs[i] = 0;
    }
    
    for (int i = 0; i < 2; i++) {
      inputs[i] = new ArrayList<FileMetaData>();
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
           Util.TotalFileSize(grandparents) <= VersionUtil.MAX_GRANDPARENT_OVERLAY_BYTES;
  }

  /**
   * Add all inputs to this compaction as delete operations to edit
   */
  public void AddInputDeletions(VersionEdit edit) {
    for (int which = 0; which < 2; which++) {
      for (int i = 0; i < inputs[which].size(); i++) {
        edit.DeleteFile(level + which, inputs[which].get(i).number);
      }
    }
  }

  /**
   * Return true if the information we have available guarantees that the
   * compaction is producing data in level + 1 for which no data exists in
   * levels greater than level + 1.
   */
  public boolean IsBaseLevelForKey(Slice user_key) {
    Comparator user_cmp = input_version.vset.icmp.user_comparator;
    for (int i = level + 2; i < Settings.NUM_LEVELS; i++) {
      ArrayList<FileMetaData> files = input_version.files[i];
      for (; level_ptrs[i] < files.size();) {
        FileMetaData file = files.get(level_ptrs[i]);
        if (user_cmp.Compare(user_key, file.largest.GetUserKey()) <= 0) {
          // we have advanced far enough
          if (user_cmp.Compare(user_key, file.smallest.GetUserKey()) >= 0) {
            // key fails in this files' range, so definitely not base level
            return false;
          }
          break;
        }
        level_ptrs[i]++;
      }
    }
    return true;
  }

  /**
   * Return true iff we should stop building the current output before
   * processing key
   */
  public boolean ShouldStopBefore(Slice key) {
    InternalKeyComparator icmp = input_version.vset.icmp;
    while (grandparent_index < grandparents.size() &&
           icmp.Compare(key, grandparents.get(grandparent_index).largest.Encode()) > 0) {
      if (seen_key) {
        overlapping_bytes += grandparents.get(grandparent_index).file_size;
      }
      grandparent_index++;
    }
    seen_key = true;
    
    if (overlapping_bytes > VersionUtil.MAX_GRANDPARENT_OVERLAY_BYTES) {
      overlapping_bytes = 0;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Release the input version for the compaction, once the compaction is
   * successful.
   */
  public void ReleaseInputs() {
    // did nothing with java
  }
}
