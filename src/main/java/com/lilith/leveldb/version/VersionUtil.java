package com.lilith.leveldb.version;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.lilith.leveldb.api.Comparator;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.Settings;

public class VersionUtil {
  public static final int TARGET_FILE_SIZE = 2 * 1048576;

  //Maximum bytes of overlaps in grandparent (i.e., level+2) before we
  //stop building a single file in a level->level+1 compaction.
  public static final long MAX_GRANDPARENT_OVERLAY_BYTES = 10 * TARGET_FILE_SIZE;

  //Maximum number of bytes in all compacted files.  We avoid expanding
  //the lower level file set of a compaction if it would make the
  //total compaction cover more than this many bytes.
  public static final long EXPANDED_COMPACTION_BYTESIZE_LIMIT = 25 * TARGET_FILE_SIZE;
  
  /**
   * We could vary per level to reduce number of files?
   */
  public static int MaxFileSizeForLevel(int level) {
    return VersionUtil.TARGET_FILE_SIZE;
  }
  
  public static double MaxBytesForLevel(int level) {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.
    double result = 10 * 1048576.0;  // Result for both level-0 and level-1
    while (level > 1) {
      result *= 10;
      level--;
    }
    return result;
  }
  
  /**
   * Return the smallest index i such that files[i].largest >= key.
   * Return files.size() if there's no such file.
   * Require: files contains a sorted list of non-overlapping  files.
   */
  public static int FindFile(InternalKeyComparator icmp, ArrayList<FileMetaData> files, Slice key) {
    int left = 0, right = files.size(), mid = 0;
    while (left < right) {
      mid = (left + right) / 2;
      FileMetaData file = files.get(mid);
      if (icmp.Compare(key, file.largest.Encode()) <= 0) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    return right;
  }
  
  /**
   * Returns true iff some file in 'files' overlaps the user key range [smallest, largest].
   * smallest == null represents a key smaller than all keys, largest == null represents a key larger
   * that all keys.
   * If disjoint_sorted_files is true, files are organized in disjoint sort. 
   */
  public static boolean SomeFileOverlapsRange(InternalKeyComparator icmp, boolean disjoint_sorted_files,
        ArrayList<FileMetaData> files, Slice smallest_user_key, Slice largest_user_key) {
    if (!disjoint_sorted_files) {
      return UnsortedFilesOverlapsRange(icmp, files, smallest_user_key, largest_user_key);
    }
    return SortedFilesOverlapsRange(icmp, files, smallest_user_key, largest_user_key);
  }
  
  private static boolean SortedFilesOverlapsRange(InternalKeyComparator icmp, ArrayList<FileMetaData> files,
      Slice smallest_user_key, Slice largest_user_key) {
    int index = 0;
    if (smallest_user_key != null) {
      InternalKey small = new InternalKey(smallest_user_key, Settings.MaxSequenceNumber, Settings.OP_TYPE_SEEK);
      index = FindFile(icmp, files, small.Encode());
    }
    
    if (index >= files.size()) return false;
    return !BeforeFile(icmp.user_comparator, largest_user_key, files.get(index));
  }
  
  private static boolean UnsortedFilesOverlapsRange(InternalKeyComparator icmp, ArrayList<FileMetaData> files,
      Slice smallest_user_key, Slice largest_user_key) {
    Iterator<FileMetaData> iter = files.iterator();
    while (iter.hasNext()) {
      FileMetaData file = iter.next();
      if (AfterFile(icmp.user_comparator, smallest_user_key, file)) continue;
      if (BeforeFile(icmp.user_comparator, largest_user_key, file)) continue;
      return true;
    }
    return false;
  }
  
  private static boolean BeforeFile(Comparator icmp, Slice user_key, FileMetaData file) {
    return user_key != null && icmp.Compare(user_key, file.smallest.GetUserKey()) < 0;
  }
  
  private static boolean AfterFile(Comparator icmp, Slice user_key, FileMetaData file) {
    return user_key != null && icmp.Compare(user_key, file.largest.GetUserKey()) > 0;
  }
}
