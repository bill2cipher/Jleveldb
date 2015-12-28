package com.lilith.leveldb.version;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import com.lilith.leveldb.api.Comparator;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.exceptions.DecodeFailedException;
import com.lilith.leveldb.memtable.LookupKey;
import com.lilith.leveldb.table.Table;
import com.lilith.leveldb.table.TableIterator;
import com.lilith.leveldb.table.TableLevelIterator;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.Settings;
import com.lilith.leveldb.util.Util;

public class Version {
  
  // List of files per level.
  ArrayList<FileMetaData>[] files = null;
  
  VersionSet vset = null;
  Version next = null;
  Version prev = null;
  
  // level should be compacted next and its compaction score. Score < 1 means
  // compaction is not strictly needed.
  double compaction_score = 0;
  int compaction_level = 0;
  
  // next file to compact based on seek states
  FileMetaData file_to_compact = null;
  int file_to_compact_level = 0;
  
  java.util.Comparator<FileMetaData> file_cmp = new java.util.Comparator<FileMetaData>() {
    public int compare(FileMetaData fval, FileMetaData sval) {
      if (fval.number < sval.number) return 1;
      if (fval.number == sval.number) return 0;
      return -1;
    }
  };
  
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
   * Append to iters a sequence of iterators that will yield the contents of 
   * this Version when merged together.
   * @throws BadFormatException 
   * @throws DecodeFailedException 
   * @throws IOException 
   */
  public void AddIterators(ReadOptions options, ArrayList<TableIterator> iters)
      throws IOException, DecodeFailedException, BadFormatException {
    // Merge all zero level files together since they may overlap
    Iterator<FileMetaData> l0_file_iter = files[0].iterator();
    while (l0_file_iter.hasNext()) {
      FileMetaData file = l0_file_iter.next();
      Table table = vset.table_cache.FindTable(file.number, file.file_size);
      iters.add(table.TableIterator(options));
    }
    
    for (int i = 1; i < Settings.NUM_LEVELS; i++) {
      ArrayList<FileMetaData> level_files = files[i];
      iters.add(new TableLevelIterator(vset.icmp, level_files, vset.table_cache, options));
    }
  }
  
  /**
   * Lookup the value for key. If found, return a slice representing
   * the value, else return null.
   * @throws BadFormatException 
   * @throws DecodeFailedException 
   * @throws IOException 
   */
  public Slice Get(ReadOptions options, LookupKey key) throws IOException, DecodeFailedException, BadFormatException {
    Slice internal_key = key.InternalKey();
    Slice user_key = key.UserKey();
    
    Comparator ucmp = vset.icmp.user_comparator;
    ArrayList<FileMetaData> tmp_files = new ArrayList<FileMetaData>();
    
    // we can search level-by-level since entries never hop across levels.
    // therefore we are guaranteed that if we find data in an smaller level,
    // later levels are irrelevant.
    for (int level = 0; level < Settings.NUM_LEVELS; level++) {
      int num_files = files[level].size();
      if (num_files == 0) continue;
      Iterator<FileMetaData> level_iter = files[level].iterator();
      
      if (level == 0) {
        while (level_iter.hasNext()) {
          FileMetaData file = level_iter.next();
          if ((ucmp.Compare(user_key, file.smallest.GetUserKey()) >= 0) &&
               ucmp.Compare(user_key, file.largest.GetUserKey()) <= 0) tmp_files.add(file);
        }
        if (tmp_files.isEmpty()) continue;
      } else {
        int file_index = VersionUtil.FindFile(vset.icmp, files[level], internal_key);
        if (file_index >= num_files) continue;
        if (ucmp.Compare(files[level].get(file_index).smallest.GetUserKey(), user_key) > 0) continue;
        tmp_files.add(files[level].get(file_index));
      }
      
      for (int i = 0; i < tmp_files.size(); i++) {
        FileMetaData file = tmp_files.get(i);
        Slice value = vset.table_cache.Get(options, file.number, file.file_size, internal_key);
        if (value == null) continue;
        return value;
      }
      tmp_files.clear();
    }
    return null;    
  }
  
  /**
   * Number of files at the specified level.
   */
  public int NumFiles(int level) {
    return files[level].size();
  }
    
  /**
   * For each file overlapping the given user_key, add it to the result list.
   */
  private ArrayList<FileMetaData> GetOverlappingFiles(Slice user_key, Slice internal_key) {
    Comparator user_comparator = vset.icmp.user_comparator;
    ArrayList<FileMetaData> result = new ArrayList<FileMetaData>();
    
    // Search level-0 in order from newest to oldest
    for (int i = 0; i < files[0].size(); i++) {
      FileMetaData file = files[0].get(i);
      if (user_comparator.Compare(user_key, file.smallest.GetUserKey()) >= 0 ||
          user_comparator.Compare(user_key, file.largest.GetUserKey()) <= 0) {
        result.add(file);
      }
    }
    
    if (!result.isEmpty()) {
      Collections.sort(result, file_cmp);
    }
    
    // Search other level
    for (int i = 0; i < Settings.NUM_LEVELS; i++) {
      int file_num = files[i].size();
      if (file_num <= 0) continue;
      int index = VersionUtil.FindFile(vset.icmp, files[i], internal_key);
      if (index < file_num) {
        FileMetaData file = files[i].get(index);
        if (user_comparator.Compare(user_key, file.smallest.GetUserKey()) < 0) {
          // all files is larger than the given user key.
        } else {
          result.add(file);
        }
      }
    }
    return result;
  }
  
  public boolean OverlapInLevel(int level, Slice smallest, Slice largest) {
    return VersionUtil.SomeFileOverlapsRange(vset.icmp, level > 0, files[level], smallest, largest);
  }
  
  /**
   * Return the level at which we should place a new memtable compaction result that
   * covers the range [smallest_key, largest_key].
   */
  public int PickLevelForMemTableOutput(Slice smallest_key, Slice largest_key) {
    int level = 0;
    if (OverlapInLevel(0, smallest_key, largest_key)) return level;
    InternalKey start = new InternalKey(smallest_key, Settings.MaxSequenceNumber, Settings.OP_TYPE_SEEK);
    InternalKey limit = new InternalKey(largest_key, 0L, (byte) 0);
    
    while (level < Settings.MaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, smallest_key, largest_key)) break;
      if (level + 2 < Settings.NUM_LEVELS) {
        ArrayList<FileMetaData> overlaps = GetOverlappingInputs(level + 2, start, limit);
        int file_size = Util.TotalFileSize(overlaps);
        if (file_size > VersionUtil.MAX_GRANDPARENT_OVERLAY_BYTES) break;
      }
      level++;
    }
    return level;
  }
  
  ArrayList<FileMetaData> GetOverlappingInputs(int level , InternalKey start, InternalKey limit) {
    ArrayList<FileMetaData> overlaps = new ArrayList<FileMetaData>();
    Slice user_start = null, user_limit = null;
    if (start != null) user_start = start.GetUserKey();
    if (limit != null) user_limit = limit.GetUserKey();
    Comparator user_cmp = vset.icmp.user_comparator;
    
    for (int i = 0; i < files[level].size(); i++) {
      FileMetaData file = files[level].get(i);
      if (user_start != null && user_cmp.Compare(user_start, file.largest.GetUserKey()) > 0) continue;
      if (user_limit != null && user_cmp.Compare(user_limit, file.smallest.GetUserKey()) < 0) continue;
      overlaps.add(file);
      if (level == 0) {
        if (user_start != null && user_cmp.Compare(file.smallest.GetUserKey(), user_start) < 0) {
          user_start = file.smallest.GetUserKey();
          i = 0; overlaps.clear();
        }
        if (user_limit != null && user_cmp.Compare(file.largest.GetUserKey(), user_limit) > 0) {
          user_limit = file.largest.GetUserKey();
          i = 0; overlaps.clear();
        }
      }
    }
    return overlaps;
  }
  
  
}
