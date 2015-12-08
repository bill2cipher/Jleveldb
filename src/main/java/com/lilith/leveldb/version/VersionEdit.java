package com.lilith.leveldb.version;

import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;

import com.lilith.leveldb.api.Slice;

public class VersionEdit {
  private String comparator;
  private ArrayList<SimpleEntry<Integer, Long>> deleted_files = null;
  private ArrayList<SimpleEntry<Integer, InternalKey>> compact_pointers = null;
  private ArrayList<SimpleEntry<Integer, FileMetaData>> new_files = null;
  
  private long log_num;
  private long prev_log_num;
  private long next_file_num;
  private long last_seq;
  
  private boolean has_comparator = false;
  private boolean has_log_num = false;
  private boolean has_next_file_num = false;
  private boolean has_last_seq = false;
    
  // Tag numbers for serialized VersionEdit. These numbers are written to disk
  // and should not be changed.
  private final static int Comparator     = 1;
  private final static int LogNumber      = 2;
  private final static int NextFileNum    = 3;
  private final static int LastSeq        = 4;
  private final static int CompactPointer = 5;
  private final static int DeletedFile    = 6;
  private final static int NewFile        = 7;
  private final static int PrevLogNum     = 9;
  
  public void Clear() {
    
  }
  
  public void SetComparatorName(String name) {
    this.comparator = name;
    this.has_comparator = true;
  }
  
  public void SetLogNumber(long num) {
    this.has_log_num = true;
    this.log_num = num;
  }
  
  public void SetNextFile(long num) {
    this.next_file_num = num;
    this.has_next_file_num = true;
  }
  
  public void SetLastSequence(long seq) {
    this.has_last_seq = true;
    this.last_seq = seq;
  }
  
  public void SetCopmactionPointer(int level, Slice key) {
    
  }
  
  /**
   * Add the specified file at the specified number.
   */
  public void AddFile(int level, long file, int file_size, Slice smallest, Slice largest) {
    FileMetaData file_meta = new FileMetaData();
    file_meta.file_size = file_size;
    file_meta.number = file;
    file_meta.smallest = smallest;
    file_meta.largest = largest;
    
    new_files.add(new SimpleEntry<Integer, FileMetaData>(level, file_meta));
  }
  
  /**
   * Delete the specified file from the specified level.
   */
  public void DeleteFile(int level, long file) {
    deleted_files.add(new SimpleEntry<Integer, Long>(level, file));
  }
  
  public Slice EncodeTo() {
    return null;
  }
}
