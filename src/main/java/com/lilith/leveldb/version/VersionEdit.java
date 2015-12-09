package com.lilith.leveldb.version;

import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class VersionEdit {
  private String comparator;
  private ArrayList<SimpleEntry<Integer, Long>> deleted_files = null;
  private ArrayList<SimpleEntry<Integer, InternalKey>> compact_pointers = null;
  private ArrayList<SimpleEntry<Integer, FileMetaData>> new_files = null;
  
  private long log_num;
  private long next_file_num;
  private long last_seq;
  
  private boolean has_log_num = false;
  private boolean has_comparator = false;
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
    this.log_num = 0;
    this.last_seq = 0;
    this.next_file_num = 0;
    
    this.has_comparator = false;
    this.has_last_seq = false;
    this.has_next_file_num = false;
    this.has_log_num = false;
    
    comparator = "";
    
    deleted_files.clear();
    new_files.clear();
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
  
  public void SetCopmactionPointer(int level, InternalKey key) {
    compact_pointers.add(new SimpleEntry<Integer, InternalKey>(level, key));
  }
  
  /**
   * Add the specified file at the specified number.
   */
  public void AddFile(int level, long file, int file_size, InternalKey smallest, InternalKey largest) {
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
    byte[] buffer = new byte[GetApproximateSize()];
    int offset = 0;
    if (has_comparator) {
      byte[] comp_content = comparator.getBytes();
      BinaryUtil.PutVarint32(buffer, offset, Comparator);
      BinaryUtil.PutVarint32(buffer, offset + Settings.UINT32_SIZE, comp_content.length);
      BinaryUtil.CopyBytes(comp_content, 0, comp_content.length, buffer, offset + Settings.UINT32_SIZE * 2);
      offset += comp_content.length + Settings.UINT32_SIZE * 2;
    }
    
    if (has_log_num) {
      BinaryUtil.PutVarint32(buffer, offset, LogNumber);
      BinaryUtil.PutVarint64(buffer, offset + Settings.UINT32_SIZE, log_num);
      offset += Settings.UINT32_SIZE + Settings.UINT64_SIZE;
    }
    
    if (has_next_file_num) {
      BinaryUtil.PutVarint32(buffer, offset, NextFileNum);
      BinaryUtil.PutVarint64(buffer, offset + Settings.UINT32_SIZE, next_file_num);
      offset += Settings.UINT32_SIZE + Settings.UINT64_SIZE;
    }
    
    if (has_last_seq) {
      BinaryUtil.PutVarint32(buffer, offset, LastSeq);
      BinaryUtil.PutVarint64(buffer, offset + Settings.UINT32_SIZE, last_seq);
      offset += Settings.UINT32_SIZE + Settings.UINT64_SIZE;
    }
    
    for (int i = 0; i < compact_pointers.size(); i++) {
      BinaryUtil.PutVarint32(buffer, offset, CompactPointer);
      BinaryUtil.PutVarint32(buffer, offset + Settings.UINT32_SIZE, compact_pointers.get(i).getKey());
      Slice key = compact_pointers.get(i).getValue().Encode();
      BinaryUtil.CopyBytes(key.GetData(), key.GetOffset(), key.GetLength(), buffer, offset + Settings.UINT32_SIZE * 2);
      offset += Settings.UINT32_SIZE * 2 + key.GetLength();
    }
    
    Iterator<SimpleEntry<Integer, Long>> del_iter = deleted_files.iterator();
    while (del_iter.hasNext()) {
      SimpleEntry<Integer, Long> val = del_iter.next();
      BinaryUtil.PutVarint32(buffer, offset, DeletedFile);
      BinaryUtil.PutVarint32(buffer, offset + Settings.UINT32_SIZE, val.getKey());
      BinaryUtil.PutVarint64(buffer, offset + Settings.UINT32_SIZE * 2, val.getValue());
      offset += Settings.UINT32_SIZE * 2 + Settings.UINT64_SIZE;
    }
  
    Iterator<SimpleEntry<Integer, FileMetaData>> add_iter = new_files.iterator();
    while (add_iter.hasNext()) {
      SimpleEntry<Integer, FileMetaData> entry = add_iter.next();
      BinaryUtil.PutVarint32(buffer, offset, NewFile); offset += Settings.UINT32_SIZE;
      BinaryUtil.PutVarint32(buffer, offset, entry.getKey()); offset += Settings.UINT32_SIZE;
      BinaryUtil.PutVarint64(buffer, offset, entry.getValue().number); offset += Settings.UINT64_SIZE;
      BinaryUtil.PutVarint64(buffer, offset, entry.getValue().file_size); offset += Settings.UINT64_SIZE;
      
      Slice small = entry.getValue().smallest.Encode();
      BinaryUtil.PutVarint32(buffer, offset, small.GetLength()); offset += Settings.UINT32_SIZE;
      BinaryUtil.CopyBytes(small.GetData(), small.GetOffset(), small.GetLength(), buffer, offset);
      
      Slice large = entry.getValue().largest.Encode();
      BinaryUtil.PutVarint32(buffer, offset, large.GetLength()); offset += Settings.UINT32_SIZE;
      BinaryUtil.CopyBytes(large.GetData(), large.GetOffset(), large.GetLength(), buffer, offset);
    }
    return new Slice(buffer);
  }
  
  public int GetApproximateSize() {
    int size = 0;
    if (has_comparator) size += Settings.UINT32_SIZE;
    if (has_log_num)    size += Settings.UINT32_SIZE;
    if (has_next_file_num) size += Settings.UINT32_SIZE;
    if (has_last_seq) size += Settings.UINT32_SIZE;
    
    for (int i = 0; i < compact_pointers.size(); i++) {
      size += Settings.UINT32_SIZE * 3;
      size += compact_pointers.get(i).getValue().GetInternalKeySize();
    }
    
    size += (Settings.UINT32_SIZE * 2 + Settings.UINT64_SIZE) * deleted_files.size();
    
    Iterator<SimpleEntry<Integer, FileMetaData>> add_iter = new_files.iterator();
    while (add_iter.hasNext()) {
      SimpleEntry<Integer, FileMetaData> entry = add_iter.next();
      size += Settings.UINT32_SIZE * 2 + Settings.UINT64_SIZE * 2;
      size += entry.getValue().largest.GetInternalKeySize() + Settings.UINT32_SIZE;
      size += entry.getValue().smallest.GetInternalKeySize() + Settings.UINT32_SIZE;
    }
    return size;
  }
}
