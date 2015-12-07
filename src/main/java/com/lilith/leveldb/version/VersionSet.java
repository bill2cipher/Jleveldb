package com.lilith.leveldb.version;

import java.io.DataOutputStream;
import java.util.List;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.Options;

public class VersionSet {
  private String dbname = null;
  
  private Options options = null;        // options for db versionset
  private TableCache table_cache = null; // used for cache table content
  
  private long manifest_file_num = 0;    // unique id for current descriptor
  private long next_file_num     = 0;    // unique id for table file
  private long last_sequence     = 0;    // sequence number for write batch
  private long log_num           = 0;    // unique id for log file
  private long prev_log_num      = 0;    // 0 or backing store for memtable being compacted
  
  private Version header = null;         // head of circular doubly-linked list of versions.
  private Version current = null;        // == header.prev
  
  private DataOutputStream desc_file_writer = null;
  private DataOutputStream desc_log_writer  = null;
  
  public VersionSet(String dbname, Options options, TableCache table_cache) {
    this.current = null;
    this.options = options;
    this.table_cache = table_cache;
  }
  
  /**
   * Apply edit to the current version to form a new descriptor that is both saved to
   * persistent state and installed as the new current version.
   */
  public void LogAndApply(VersionEdit edit) {
    
  }
  
  /**
   * Recover the last saved descriptor from persistent storage.
   */
  public void Recover() {
    
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
