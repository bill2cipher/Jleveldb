package com.lilith.leveldb.version;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.exceptions.DecodeFailedException;
import com.lilith.leveldb.log.LogReader;
import com.lilith.leveldb.log.LogWriter;
import com.lilith.leveldb.table.Table;
import com.lilith.leveldb.table.TableCache;
import com.lilith.leveldb.table.TableIterator;
import com.lilith.leveldb.table.TableLevelIterator;
import com.lilith.leveldb.table.TableMergeIterator;
import com.lilith.leveldb.util.FileName;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.Settings;
import com.lilith.leveldb.util.Util;

public class VersionSet {
  

  
  private String dbname = null;          // db this version set associate with
  
  private Options options = null;        // options for db versionset
  
  TableCache table_cache = null; // used for cache table content
  
  InternalKeyComparator icmp = null;
  
  // Pre-level key at which the next compaction at that level should start.
  // Either null or a valid InternalKey
  Slice[] compact_pointers = new Slice[Settings.NUM_LEVELS];
  
  private long manifest_file_num = 0;    // unique id for current descriptor
  private long next_file_num     = 0;    // unique id for table file
  private long last_sequence     = 0;    // sequence number for write batch
  private long log_num           = 0;    // unique id for log file
  
  private Version header = null;         // head of circular doubly-linked list of versions.
  private Version current = null;        // == header.prev
  
  private LogWriter descriptor_log = null;
  private DataOutputStream descriptor_file = null;  // the file associate with desc_writer
  
  
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
   * @throws BadFormatException 
   * @throws IOException 
   */
  public void LogAndApply(VersionEdit edit) throws BadFormatException, IOException {
    if (!edit.has_log_num) edit.SetLogNumber(log_num);
    edit.SetNextFile(next_file_num);
    edit.SetLastSequence(last_sequence);
    
    Version version = new Version(this);
    {
      VersionBuilder builder = new VersionBuilder(this, current);
      builder.Apply(edit);
      builder.GetCurrentVersion(version);
    }
    CalCompactionScore(version);
    
    String new_manifest_file;
    if (descriptor_log == null) {
      new_manifest_file = FileName.DescriptorFileName(dbname, manifest_file_num);
      edit.SetNextFile(next_file_num);
      descriptor_file = new DataOutputStream(new FileOutputStream(new_manifest_file));
      descriptor_log = new LogWriter(descriptor_file);
      WriteSnapshot(descriptor_log);
    }
    FileName.SetCurrentFile(dbname, manifest_file_num);
    
    Slice encode_edit = edit.EncodeTo();
    descriptor_log.AddRecord(encode_edit);
    
    AppendVersion(version);
    log_num = edit.log_num;
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
    int read_cnt = cur_reader.read(buffer, 0, 512);
    cur_reader.close();
    if (buffer[read_cnt - 1] != '\n') throw new BadFormatException("CURRENT file does not end with newline");
    
    
    String descname = new String(buffer, 0, read_cnt - 1);
    DataInputStream desc_reader = new DataInputStream(new FileInputStream(dbname + "/" + descname));
    
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
  
  private void WriteSnapshot(LogWriter writer) throws IOException {
    VersionEdit edit = new VersionEdit();
    String comparator_name = icmp.user_comparator.Name();
    edit.SetComparatorName(new Slice(comparator_name.getBytes()));

    // save compaction pointers
    for (int level = 0; level < Settings.NUM_LEVELS; level++) {
      if (!(compact_pointers[level] == null)) {
        InternalKey internal_key = new InternalKey();
        Slice cmp_ptr = compact_pointers[level];
        internal_key.DecodeFrom(cmp_ptr.GetData(), cmp_ptr.GetOffset(), cmp_ptr.GetLength());
        edit.SetCopmactionPointer(level, internal_key);
      }
    }
    
    // Save files
    for (int level = 0; level < Settings.NUM_LEVELS; level++) {
      ArrayList<FileMetaData> files = current.files[level];
      Iterator<FileMetaData> file_iter = files.iterator();
      while (file_iter.hasNext()) {
        FileMetaData file_meta = file_iter.next();
        edit.AddFile(level, file_meta.number, file_meta.file_size, file_meta.smallest, file_meta.largest);
      }
    }
    
    Slice encode_edit = edit.EncodeTo();
    writer.AddRecord(encode_edit);
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
    return manifest_file_num;
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
    return current.files[level].size();
  }
  
  /**
   * Return the combined file size of all files at the specified level.
   */
  public long NumLevelBytes(int level) {
    return Util.TotalFileSize(current.files[level]);
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
    return log_num;
  }
  
  /**
   * Stores the minimal range that covers all entries in inputs in smallest and largest.
   */
  public void GetRange(ArrayList<FileMetaData> inputs, InternalKey smallest, InternalKey largest) {
    InternalKey tmp_small = null, tmp_large = null;
    for (int i = 0; i < inputs.size(); i++) {
      FileMetaData file = inputs.get(i);
      if (i == 0) {
        tmp_small = file.smallest;
        tmp_large = file.largest;
      } else {
        if (icmp.Compare(file.smallest, smallest) < 0) tmp_small = file.smallest;
        if (icmp.Compare(file.largest, largest) > 0) tmp_large = file.largest;
      }
    }
    smallest.Clone(tmp_small);
    largest.Clone(tmp_large);
  }
  
  public void GetRange2(ArrayList<FileMetaData> inputs1, ArrayList<FileMetaData> inputs2
                      , InternalKey smallest, InternalKey largest) {
    ArrayList<FileMetaData> all = new ArrayList<FileMetaData>();
    all.addAll(inputs2);
    all.addAll(inputs2);
    GetRange(all, smallest, largest);
  }
    
  /**
   * Pick level and inputs for a new compaction. Return null if there is no
   * compaction to be done. Otherwise returns an object describes the compaction.
   */
  public Compaction PickCompaction() {
    Compaction compaction = null;
    int level = 0;
    
    boolean size_compaction = current.compaction_score >= 1;
    boolean seek_compaction = current.file_to_compact != null;
    if (size_compaction) {
      level = current.compaction_level;
      compaction = new Compaction(level);
      // Pick the first file that comes after compaction_pointer_[level]
      for (int i = 0; i < current.files[level].size(); i++) {
        FileMetaData file = current.files[level].get(i);
        if (compact_pointers[level] == null || icmp.Compare(file.largest.Encode(), compact_pointers[level]) > 0) {
          compaction.inputs[0].add(file);
          break;
        }
      }
      
      if (compaction.inputs[0].isEmpty()) {
        // wrap around to the beginning of the key space
        compaction.inputs[0].add(current.files[level].get(0));
      }
    } else if (seek_compaction) {
      level = current.file_to_compact_level;
      compaction = new Compaction(level);
      compaction.inputs[0].add(current.file_to_compact);
    } else {
      return null;
    }
    
    compaction.input_version = current;
    // files in level 0 may overlap each other, so pick up all overlapping ones
    if (level == 0) {
      InternalKey smallest = new InternalKey(), largest = new InternalKey();
      GetRange(compaction.inputs[0], smallest, largest);
      compaction.inputs[0] = current.GetOverlappingInputs(level, smallest, largest);
    }
    SetupOtherInputs(compaction);
    return compaction;
  }
  
  /**
   * Return a compaction object for compacting the range[begin,end] in the specified level.
   * Returns null if there is nothing in that level overlaps the specified range.
   */
  public Compaction CompactRange(int level, InternalKey begin, InternalKey end) {
    ArrayList<FileMetaData> inputs = current.GetOverlappingInputs(level, begin, end);
    if (inputs.isEmpty()) {
      return null;
    }
    
    // Avoid compacting too much in one shot in case the range is large.
    // But we cannot do this for level0 since level0 files can overlap and we must not
    // pick one file and drop another older file if the two files overlap
    if (level > 0) {
      long limit = VersionUtil.MaxFileSizeForLevel(level);
      long total = 0;
      for (int i = 0; i < inputs.size(); i++) {
        total += inputs.get(i).file_size;
        if (total >= limit) {
          inputs.subList(0, i + 1);
          break;
        }
      }
    }
    
    Compaction c = new Compaction(level);
    c.input_version = current;
    c.inputs[0] = inputs;
    SetupOtherInputs(c);
    return c;
  }
  
  public TableMergeIterator MakeInputIterator(Compaction c) throws IOException, DecodeFailedException, BadFormatException {
    ReadOptions rd_options = new ReadOptions();
    rd_options.verify_checksums = options.paranoid_checks;
    rd_options.fill_cache = false;
    
    int space = (c.Level() == 0 ? c.inputs[0].size() + 1 : 2);
    ArrayList<TableIterator> list = (ArrayList<TableIterator>) new ArrayList(space);
    int num = 0;
    for (int which = 0; which < 2; which++) {
      if (c.inputs[which].isEmpty()) continue;
      if (c.Level() + which == 0) {
        Iterator<FileMetaData> file_iter = c.inputs[which].iterator(); 
        while (file_iter.hasNext()) {
          FileMetaData file = file_iter.next();
          Table table = table_cache.FindTable(file.number, file.file_size);
          list.set(num++, table.TableIterator(rd_options));
        }
      } else {
        list.set(num++, new TableLevelIterator(icmp, c.inputs[which], table_cache, rd_options));
      }
    }
    return new TableMergeIterator(icmp, list);
  }
  
  public void SetupOtherInputs(Compaction c) {
    final int level = c.Level();
    InternalKey smallest = new InternalKey();
    InternalKey largest = new InternalKey();
    GetRange(c.inputs[0], smallest, largest);
    c.inputs[1] = current.GetOverlappingInputs(level, smallest, largest);
    
    InternalKey all_start = new InternalKey();
    InternalKey all_limit = new InternalKey();
    GetRange2(c.inputs[0], c.inputs[1], all_start, all_limit);
    
    // see if we can grow the number of inputs in level without changing the number of
    // level + 1 files we pick up
    if (!c.inputs[0].isEmpty()) {
      ArrayList<FileMetaData> expanded0 = current.GetOverlappingInputs(level, all_start, all_limit);
      long inputs0_size = Util.TotalFileSize(c.inputs[0]);
      long inputs1_size = Util.TotalFileSize(c.inputs[1]);
      long expanded0_size = Util.TotalFileSize(expanded0);
      if (expanded0.size() > c.inputs[0].size() &&
          inputs1_size + expanded0_size < VersionUtil.EXPANDED_COMPACTION_BYTESIZE_LIMIT) {
        InternalKey new_start = new InternalKey();
        InternalKey new_limit = new InternalKey();
        GetRange(expanded0, new_start, new_limit);
        ArrayList<FileMetaData> expanded1 = current.GetOverlappingInputs(level + 1, new_start, new_limit);
        if (expanded1.size() == c.inputs[1].size()) {
          smallest = new_start;
          largest = new_limit;
          c.inputs[0] = expanded0;
          c.inputs[1] = expanded1;
          GetRange2(c.inputs[0], c.inputs[1], all_start, all_limit);
        }
      }
    }
    
    if (level + 2 < Settings.NUM_LEVELS) {
      c.grandparents = current.GetOverlappingInputs(level + 2, all_start, all_limit);
    }
    
    compact_pointers[level] = largest.Encode();
    c.Edit().SetCopmactionPointer(level, largest);
  }
  
  /**
   * Return the maximum overlapping data at next level for any file at a level >= 1.
   */
  public long MaxNextLevelOverlappingBytes() {
    long result = 0;
    ArrayList<FileMetaData> overlaps;
    for (int level = 1; level < Settings.NUM_LEVELS - 1; level++) {
      for (int i = 0; i < current.files[level].size(); i++) {
        FileMetaData file = current.files[level].get(i);
        overlaps = current.GetOverlappingInputs(level + 1, file.smallest, file.largest);
        int sum = Util.TotalFileSize(overlaps);
        if (sum > result) result = sum;
      }
    }
    return result;
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
   * @throws BadFormatException 
   * @throws DecodeFailedException 
   * @throws IOException 
   */
  public long ApproximateOffsetOf(Version version, InternalKey ikey) throws IOException, DecodeFailedException, BadFormatException {
    long result = 0;
    for (int level = 0; level < Settings.NUM_LEVELS; level++) {
      ArrayList<FileMetaData> files = version.files[level];
      Iterator<FileMetaData> file_iter = files.iterator();
      while (file_iter.hasNext()) {
        FileMetaData file = file_iter.next();
        if (icmp.Compare(file.largest, ikey) <= 0) {
          result += file.file_size;
        } else if (icmp.Compare(file.smallest, ikey) > 0) {
          if (level > 0) break;
        } else {
          Table table = table_cache.FindTable(file.number, file.file_size);
          result += table.ApproximateOffsetOf(ikey.Encode());
        }
      }
    }
    return result;
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
        long level_bytes = Util.TotalFileSize(version.files[level]);
        score = level_bytes / (double) VersionUtil.MaxBytesForLevel(level);
      }
      if (score > best_score) {
        best_level = level;
        best_score = score;
      }
    }
    version.compaction_level = best_level;
    version.compaction_score = best_score;
  }
  
  public void Close() {
    try {
      descriptor_file.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
