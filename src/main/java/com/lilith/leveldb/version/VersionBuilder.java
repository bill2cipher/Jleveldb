package com.lilith.leveldb.version;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.util.Settings;

/**
 * A helper class so we can efficiently apply a whole sequence of edit to a particular state
 * without creating intermediate Version. 
 */
public class VersionBuilder {
  private VersionSet vset = null;
  private Version base = null;
  private LevelState[] levels = null;
  private FileMetaComparator file_comparator = null;
  
  private class LevelState {
    ArrayList<Long> deleted_files;
    TreeSet<FileMetaData> added_files = null;
  }
  
  private class FileMetaComparator implements Comparator<FileMetaData> {
    private InternalKeyComparator internal_comparator = null;
    
    public FileMetaComparator(InternalKeyComparator internal_comparator) {
      this.internal_comparator = internal_comparator;
    }

    public int compare(FileMetaData ffile, FileMetaData sfile) {
      int res = internal_comparator.compare(ffile.smallest, sfile.smallest);
      if (res != 0) return res;
      else if (ffile.number == sfile.number) return 0;
      else if (ffile.number < sfile.number) return -1;
      return 1;
    }
  }
  
  // Initialize a builder with the files from base and other info from vset
  public VersionBuilder(VersionSet vset, Version base) {
    this.vset = vset;
    this.base = base;
    this.levels = new LevelState[Settings.NUM_LEVELS];
    this.file_comparator = new FileMetaComparator(vset.icmp);

    for (int level = 0; level < Settings.NUM_LEVELS; level++) {
      levels[level] = new LevelState();
      levels[level].added_files = new TreeSet<FileMetaData>(file_comparator);
      levels[level].deleted_files = new ArrayList<Long>();
    }
  }
  
  // Apply all of the edits in edit to the current state
  public void Apply(VersionEdit edit) {
    // update compaction pointers
    Iterator<SimpleEntry<Integer, InternalKey>> cmp_iter = edit.compact_pointers.iterator();
    while (cmp_iter.hasNext()) {
      SimpleEntry<Integer, InternalKey> cmp_entry = cmp_iter.next();
      int cmp_level = cmp_entry.getKey();
      Slice cmp_val = cmp_entry.getValue().Encode();
      vset.compact_pointers[cmp_level] = new String(cmp_val.GetData(), cmp_val.GetOffset(), cmp_val.GetLength());
    }
    
    // Delete files
    Iterator<SimpleEntry<Integer, Long>> del_iter = edit.deleted_files.iterator();
    while (del_iter.hasNext()) {
      SimpleEntry<Integer, Long> del_entry = del_iter.next();
      levels[del_entry.getKey()].deleted_files.add(del_entry.getValue());
    }
    
    // Add new files
    Iterator<SimpleEntry<Integer, FileMetaData>> add_iter = edit.new_files.iterator();
    while (add_iter.hasNext()) {
      SimpleEntry<Integer, FileMetaData> add_entry = add_iter.next();
      int add_level = add_entry.getKey();
      FileMetaData add_val = add_entry.getValue();
      levels[add_level].added_files.add(add_val);
      levels[add_level].deleted_files.remove(add_val.number);
    }
  }
  
  /** Merge the set of added files with the set of pre-existing files.
   * Drop any deleted files. Return the result. 
   * @throws BadFormatException 
   */
  public void GetCurrentVersion(Version version) throws BadFormatException {
    for (int level = 0; level < Settings.NUM_LEVELS; level++) {
      ArrayList<FileMetaData> base_files = base.files[level];
      Iterator<FileMetaData> add_iter = levels[level].added_files.iterator();
      Iterator<FileMetaData> base_iter = base_files.iterator();
      FileMetaData add_cur = null, base_cur = null;
      while (add_iter.hasNext() && base_iter.hasNext()) {
        if (add_cur == null) add_cur = add_iter.next();
        if (base_cur == null) base_cur = base_iter.next();
        int cmp_res = file_comparator.compare(add_cur, base_cur);
        
        if (cmp_res <= 0) {
          MaybeAddFile(version, level, add_cur);
          add_cur = null;
        }
        else if (cmp_res > 0) {
          MaybeAddFile(version, level, base_cur);
          base_cur = null;
        }
      }
      
      if (add_cur != null) MaybeAddFile(version, level, add_cur);
      if (base_cur != null) MaybeAddFile(version, level, base_cur);
      
      while (add_iter.hasNext())  MaybeAddFile(version, level, add_iter.next());
      while (base_iter.hasNext()) MaybeAddFile(version, level, base_iter.next());
    }
  }
  
  private void MaybeAddFile(Version version, int level, FileMetaData file) throws BadFormatException {
    if (levels[level].deleted_files.contains(file)) {
      // File is deleted: do nothing
    } else {
      ArrayList<FileMetaData> files = version.files[level];
      if (level > 0 && !files.isEmpty()) {
        if (vset.icmp.compare(files.get(files.size() - 1).largest, file.smallest) >= 0)
          throw new BadFormatException("overlapping files");
      }
      version.files[level].add(file);
    }
  }
}
