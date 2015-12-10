package com.lilith.leveldb.version;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.log.LogReader;
import com.lilith.leveldb.log.LogWriter;
import com.lilith.leveldb.util.FileName;
import com.lilith.leveldb.util.Options;
import  com.lilith.leveldb.util.Settings;

public class VersionSet {
  private String dbname = null;          // db this version set associate with
  
  private Options options = null;        // options for db versionset
  private TableCache table_cache = null; // used for cache table content
  
  private InternalKeyComparator icmp = null;
  
  private long manifest_file_num = 0;    // unique id for current descriptor
  private long next_file_num     = 0;    // unique id for table file
  private long last_sequence     = 0;    // sequence number for write batch
  private long log_num           = 0;    // unique id for log file
  
  private Version header = null;         // head of circular doubly-linked list of versions.
  private Version current = null;        // == header.prev
  
  private LogWriter descriptor_file = null;
  private DataOutputStream descriptor_log = null;  // the file associate with desc_writer
  
  private String[] compact_pointer = new String[Settings.NUM_LEVELS];
  
  
  public VersionSet(String dbname, Options options, TableCache table_cache, InternalKeyComparator icmp) {
    this.dbname = dbname;
    this.options = options;
    this.table_cache = table_cache;
    this.icmp = icmp;
    
    this.next_file_num = 2;
    this.manifest_file_num = 0;
    this.last_sequence = 0;
    this.log_num = 0;
    
    this.descriptor_file = null;
    this.descriptor_log = null;
    
    this.current = null;
    this.header = new Version(this);
  }
  
  /**
   * Apply edit to the current version to form a new descriptor that is both saved to
   * persistent state and installed as the new current version.
   */
  public void LogAndApply(VersionEdit edit) {
    
  }
  
  /**
   * Recover the last saved descriptor from persistent storage.
   * @throws IOException 
   * @throws BadFormatException 
   */
  public void Recover() throws IOException, BadFormatException {
    // Read "CURRENT" file, which contains a pointer to the current manifest file
    byte[] buffer = new byte[1024];
    DataInputStream cur_reader = new DataInputStream(new FileInputStream(FileName.CurrentFileName(dbname)));
    int read_cnt = cur_reader.read(buffer, 0, 1024);
    cur_reader.close();
    if (buffer[read_cnt - 1] != '\n') throw new BadFormatException("CURRENT file does not end with newline");
    
    
    String descname = dbname + "/" + new String(buffer, 0, read_cnt - 1);
    DataInputStream desc_reader = new DataInputStream(new FileInputStream(descname));

    boolean have_log_num = false;
    boolean have_prev_log_num = false;
    boolean have_next_file = false;
    boolean have_last_seq = false;
    long next_file = 0;
    long last_sq = 0;
    long log_num = 0;
    long prev_log_num = 0;
    
    LogReader reader = new LogReader(desc_reader, true, 0);
    while (true) {
      byte[] record = reader.ReadRecord();
      if (record == null) break;
      VersionEdit edit = new VersionEdit();
      edit.DecodeFrom(record);
      
      builder.Apply(edit);
      
      if (edit.has_log_number) {
        log_num = edit.log_num;
        have_log_num = true;
      }
      
      if (edit.has_prev_log_num) {
        prev_log_num = edit.prev_log_num;
        have_prev_log_num = true;
      }
      
      if (edit.has_next_file_num) {
        next_file_num = edit.next_file_num;
        have_next_file = true;
      }
      
      if (edit.has_last_seq) {
        last_seq = edit.last_seq;
        have_last_seq = true;
      }
    }
    desc_reader.close();
    
    Version version = new Version(this);
    builder.SaveTo(v);
  }
  
  /**
   * Return the current verions.
   */
  public Version Current() {
    return current;
  }
  
  /**
   * Return the current manifest file number 
   */
  public long ManifestFileNumber() {
    return manifest_file_num++;
  }
  
  public long NewFileNumber() {
    return next_file_num++;
  }
  
  /**
   * Arrange to reuse 'file_num" unless a newer file number has already been
   * allocated.
   */
  public void ReuseFileNumber(long file_num) {
    if (next_file_num == file_num + 1) next_file_num = file_num;
  }
  
  /**
   * Return the number of Table files at the specified level.
   */
  public int NumLevelFiles(int level) {
    return 0;
  }
  
  /**
   * Return the combined file size of all files at the specified level.
   */
  public long NumLevelBytes(int level) {
    return 0;
  }
  
  /**
   * Return the last sequence number;
   */
  public long lastSequence() {
    return last_sequence;
  }
  
  /**
   * Set the last sequence number to seq.
   */
  public void SetLastSequence(long seq) {
    this.last_sequence = seq;
  }
  
  /**
   * Mark the specified file number as used;
   */
  public void MarkeFileNumberUsed(long number) {
    
  }
  
  /**
   * Return the current log file number.
   */
  public long LogNumber() {
    return log_num++;
  }
  
  /**
   * Return the log file number for the log file that is currently being
   * compacted, or zero if there's no such a log file.
   */
  public long PrevLogNumber() {
    return prev_log_num;
  }
  
  /**
   * Pick level and inputs for a new compaction. Return null if there is no
   * compaction to be done. Otherwise returns an object describes the compaction.
   */
  public Compaction PickCompaction() {
    return null;
  }
  
  /**
   * Return a compaction object for compacting the range[begin,end] in the specified level.
   * Returns null if there is nothing in that level overlaps the specified range.
   */
  public Compaction CompactRange(int level, Slice begin, Slice end) {
    return null;
  }
  
  /**
   * Return the maximum overlapping data at next level for any file at a level >= 1.
   */
  public long MaxNextLevelOverlappingBytes() {
    return 0;
  }
  
  public boolean NeedCompaction() {
    Version v = current;
    return (v.compaction_score >= 1) || (v.file_to_compact != null);
  }
  
  public void AddliveFiles(List<Long> live) {
    
  }
  
  /**
   * Return the approximate offset in the database of the data for "key" as
   * of version "v"
   */
  public long ApproximateOffsetOf(Version v, Slice key) {
    return 0;
  }
  
  
}
