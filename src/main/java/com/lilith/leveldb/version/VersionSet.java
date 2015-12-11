package com.lilith.leveldb.version;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.log.LogReader;
import com.lilith.leveldb.log.LogWriter;
import com.lilith.leveldb.util.FileName;
import com.lilith.leveldb.util.Options;
import  com.lilith.leveldb.util.Settings;

public class VersionSet {
  
  private static final int TARGET_FILE_SIZE = 2 * 1048576;

  //Maximum bytes of overlaps in grandparent (i.e., level+2) before we
  //stop building a single file in a level->level+1 compaction.
  private static final long MAX_GRANDPARENT_OVERLAY_BYTES = 10 * TARGET_FILE_SIZE;

  //Maximum number of bytes in all compacted files.  We avoid expanding
  //the lower level file set of a compaction if it would make the
  //total compaction cover more than this many bytes.
  private static final long EXPANDED_COMPACTION_BYTESIZE_LIMIT = 25 * TARGET_FILE_SIZE;
  
  private String dbname = null;          // db this version set associate with
  
  private Options options = null;        // options for db versionset
  private TableCache table_cache = null; // used for cache table content
  
  InternalKeyComparator icmp = null;
  
  // Pre-level key at which the next compaction at that level should start.
  // Either null or a valid InternalKey
  String[] compact_pointers = new String[Settings.NUM_LEVELS];
  
  private long manifest_file_num = 0;    // unique id for current descriptor
  private long next_file_num     = 0;    // unique id for table file
  private long last_sequence     = 0;    // sequence number for write batch
  private long log_num           = 0;    // unique id for log file
  
  private Version header = null;         // head of circular doubly-linked list of versions.
  private Version current = null;        // == header.prev
  
  private LogWriter descriptor_file = null;
  private DataOutputStream descriptor_log = null;  // the file associate with desc_writer
  
  
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
    
    this.AppendVersion(new Version(this));
  }
  
  public void AppendVersion(Version version) {
    current = version;
    version.prev = header.prev;
    version.next = header;
    version.prev.next = version;
    version.next.prev = version;
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
    byte[] buffer = new byte[512];
    DataInputStream cur_reader = new DataInputStream(new FileInputStream(FileName.CurrentFileName(dbname)));
    int read_cnt = cur_reader.read(buffer, 0, 1024);
    cur_reader.close();
    if (buffer[read_cnt - 1] != '\n') throw new BadFormatException("CURRENT file does not end with newline");
    
    
    String descname = dbname + "/" + new String(buffer, 0, read_cnt - 1);
    DataInputStream desc_reader = new DataInputStream(new FileInputStream(descname));
    
    long next_file = 0;
    long last_seq_tmp = 0;
    long log_num_tmp = 0;
    
    LogReader reader = new LogReader(desc_reader, true, 0);
    VersionBuilder builder = new VersionBuilder(this, current);
    
    while (true) {
      byte[] record = reader.ReadRecord();
      if (record == null) break;
      VersionEdit edit = new VersionEdit();
      edit.DecodeFrom(record, 0, record.length);
      
      builder.Apply(edit);
      
      if (edit.has_log_num) {
        log_num_tmp = edit.log_num;
      }
      
      if (edit.has_next_file_num) {
        next_file = edit.next_file_num;
      }
      
      if (edit.has_last_seq) {
        last_seq_tmp = edit.last_seq;
      }
    }
    desc_reader.close();
    MarkFileNumberUsed(log_num);
    
    Version version = new Version(this);
    builder.GetCurrentVersion(version);
    AppendVersion(version);
    CalCompactionScore(version);
    
    manifest_file_num = next_file;
    next_file_num = next_file + 1;
    last_sequence = last_seq_tmp;
    log_num = log_num_tmp;
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
  public void MarkFileNumberUsed(long number) {
    if (number > next_file_num) next_file_num = number + 1;
  }
  
  /**
   * Return the current log file number.
   */
  public long LogNumber() {
    return log_num++;
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
  
  public void AddliveFiles(Set<Long> live) {
    Version cur = header.next;
    while (cur != header) {
      for (int i = 0; i < Settings.NUM_LEVELS; i++) {
        Iterator<FileMetaData> iter = cur.files[i].iterator();
        while (iter.hasNext()) live.add(iter.next().number);
      }
      cur = cur.next;
    }
  }
  
  /**
   * Return the approximate offset in the database of the data for "key" as
   * of version "v"
   */
  public long ApproximateOffsetOf(Version v, Slice key) {
    return 0;
  }
  
  /**
   * precomputed best level for next compaction
   */
  private void CalCompactionScore(Version version) {
    int best_level = -1;
    double best_score = -1;
    for (int level = 0; level < Settings.NUM_LEVELS - 1; level++) {
      double score = 0;
      if (level == 0) {
        // We treat level0 specially by bounding the number of files instead number of bytes for two reasons:
        // (1) With larger write-buffer sizes, it is nice not to do too many level-0 compaction
        // (2) The files in level-0 are merged on every read and therefore we wish to avoid too many files when
        //     the individual file size is small (perhaps because of a small write-buffer setting, or very
        //     high compression ratios, or lots of overwrites/deletions).
        score = version.files[level].size() / (double) Settings.L0_COMPACTION_TRIGGER;
      } else {
        long level_bytes = TotalFileSize(version.files[level]);
        score = level_bytes / (double) MaxBytesForLevel(level);
      }
      if (score > best_score) {
        best_level = level;
        best_score = score;
      }
    }
    version.compaction_level = best_level;
    version.compaction_score = best_score;
  }
  
  private long TotalFileSize(ArrayList<FileMetaData> files) {
    long size = 0;
    Iterator<FileMetaData> iter = files.iterator();
    while (iter.hasNext()) {
      size += iter.next().file_size;
    }
    return size;
  }
  
  /**
   * We could vary per level to reduce number of files?
   */
  private int MaxFileSizeForLevel(int level) {
    return TARGET_FILE_SIZE;
  }
  
  static double MaxBytesForLevel(int level) {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.
    double result = 10 * 1048576.0;  // Result for both level-0 and level-1
    while (level > 1) {
      result *= 10;
      level--;
    }
    return result;
  }
}
