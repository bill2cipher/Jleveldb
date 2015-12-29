package com.lilith.leveldb.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import com.lilith.leveldb.api.DBIterator;
import com.lilith.leveldb.api.LevelDB;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.api.SliceComparator;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.exceptions.DecodeFailedException;
import com.lilith.leveldb.log.LogReader;
import com.lilith.leveldb.log.LogWriter;
import com.lilith.leveldb.memtable.LookupKey;
import com.lilith.leveldb.memtable.MemIterator;
import com.lilith.leveldb.memtable.MemTable;
import com.lilith.leveldb.table.TableBuilder;
import com.lilith.leveldb.table.TableCache;
import com.lilith.leveldb.table.TableMergeIterator;
import com.lilith.leveldb.util.FileLocker;
import com.lilith.leveldb.util.FileName;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.Range;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.Settings;
import com.lilith.leveldb.util.Util;
import com.lilith.leveldb.util.WriteOptions;
import com.lilith.leveldb.version.Compaction;
import com.lilith.leveldb.version.FileMetaData;
import com.lilith.leveldb.version.InternalKey;
import com.lilith.leveldb.version.InternalKeyComparator;
import com.lilith.leveldb.version.Version;
import com.lilith.leveldb.version.VersionSet;
import com.lilith.leveldb.version.VersionEdit;

public class LevelDBImpl extends LevelDB {
  
  public static final int NUM_NONTABLE_CACHE_FILES = 10;

  public Options options = null;
  public String dbname = null;

  public VersionEdit version_edit = null;
  public VersionSet version_set = null;

  public DataOutputStream log_file = null;
  public long log_num = 0;
  public LogWriter log = null;

  private MemTable mem = null;
  private MemTable imm = null;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  private HashSet<Long> pending_outputs = null;

  private ArrayList<Writer> writers = null;

  private InternalKeyComparator internal_comparator = null;

  private TableCache table_cache = null;

  private boolean bg_compaction_scheduled; // flag indicating if an compaction thread is already on.
  private boolean shutting_down; // flag indicating if the server is being deleted.
  
  private FileLocker locker = null;
  
  private CompactionStats[] stats = null;
  
  private ManualCompaction manual_compaction = null;
  
  private LinkedList<Long> snapshots = null;

  private class Writer {
    public WriteBatch batch = null;
    public boolean sync = false;
    public boolean done = false;
  }
  
  private class ManualCompaction {
    public int level;
    public boolean done;
    public InternalKey begin;
    public InternalKey end;
    public InternalKey tmp_storage;
  }

  public LevelDBImpl(Options options, String dbname) {
    this.options = options;
    this.dbname = dbname;
    this.internal_comparator = new InternalKeyComparator(new SliceComparator());
    this.pending_outputs = new HashSet<Long>();
    this.writers = new ArrayList<Writer>();
    this.table_cache = new TableCache(dbname, options, options.max_open_files - NUM_NONTABLE_CACHE_FILES);
    this.version_set = new VersionSet(dbname, options, table_cache, internal_comparator);
    this.mem = new MemTable(internal_comparator);
    
    this.log_num = 0;
    this.log = null;
    
    this.bg_compaction_scheduled = false;
    this.stats = new CompactionStats[Settings.NUM_LEVELS];
    this.snapshots = new LinkedList<Long>();
  }

  @Override
  public void Put(WriteOptions options, Slice key, Slice value) throws IOException, BadFormatException, DecodeFailedException {
    WriteBatch batch = new WriteBatch(0);
    batch.Put(key, value);
    Write(options, batch);
  }

  @Override
  public void Delete(WriteOptions options, Slice key) throws IOException, BadFormatException, DecodeFailedException {
    WriteBatch batch = new WriteBatch(0);
    batch.Delete(key);
    Write(options, batch);
  }

  @Override
  public void Write(WriteOptions options, WriteBatch updates) throws IOException, BadFormatException, DecodeFailedException {
    Writer writer = new Writer();
    writer.batch = updates;
    writer.sync = options.sync;
    writer.done = false;
    writers.add(writer);

    // while (!writer.done && writers.get(0) != writer) {
    // writer.wait();
    // }

    MakeRoomForWrite(updates == null);
    long last_sequence = version_set.lastSequence();
    Writer last_writer = null;

    if (updates != null) {
      updates = new WriteBatch(last_sequence + 1);
      last_writer = BuildBatchGroup(updates);
      last_sequence += updates.Count();
      
      log.AddRecord(updates.Contents());
      if (options.sync) log_file.flush();
      updates.InsertInto(mem);
      version_set.SetLastSequence(last_sequence);
    }
    
    Iterator<Writer> iter = writers.iterator();
    while (iter.hasNext()) {
      if (iter.next() == last_writer) iter.remove();
      break;
    }
  }

  /**
   * Recover the descriptor from persistent storage. May do a significant amount
   * of work to recover recently logged updates. Any changes to be made to the
   * descriptor are added to edit.
   * 
   * @throws IOException
   * @throws BadFormatException
   */
  public VersionEdit Recover() throws IOException, BadFormatException {
    if (!CheckDBOptions())
      return null;

    version_set.Recover();
    // Recover from all newer log files than the ones named in the descriptor
    // (new log files may have been added by the previous incarnation without
    // registering them in descriptor).
    long min_log = version_set.LogNumber();
    ArrayList<String> filenames = new ArrayList<String>();
    Util.GetChildren(dbname, filenames);

    HashSet<Long> expected = new HashSet<Long>();
    version_set.AddliveFiles(expected);

    // Parse all log files.
    ArrayList<Long> logs = new ArrayList<Long>();
    for (int i = 0; i < filenames.size(); i++) {
      int file_type = FileName.ParseFileType(filenames.get(i));
      long file_num = FileName.ParseFileNumber(filenames.get(i), file_type);
      expected.remove(file_num);
      if (file_type == FileName.LOG_FILE && (file_num > min_log))
        logs.add(file_num);
    }

    if (!expected.isEmpty())
      throw new BadFormatException("missing files");

    // Recover in the order in which the logs were generated.
    Collections.sort(logs);
    VersionEdit edit = new VersionEdit();
    Iterator<Long> log_iter = logs.iterator();
    long max_seq = 0;
    while (log_iter.hasNext()) {
      long num = log_iter.next();
      long max_seq_tmp = RecoverLogFile(num, edit);
      version_set.MarkFileNumberUsed(num);
      if (max_seq_tmp > max_seq)
        max_seq = max_seq_tmp;
    }

    if (version_set.lastSequence() < max_seq)
      version_set.SetLastSequence(max_seq);

    return edit;
  }

  /**
   * Delete files that's no longer needed.
   */
  public void DeleteObsoleteFiles() {
    // make a set of all the live files.
    Set<Long> live = new HashSet<Long>(pending_outputs);
    version_set.AddliveFiles(live);

    ArrayList<String> filenames = new ArrayList<String>();
    Util.GetChildren(dbname, filenames);
    long file_num;
    int file_type;

    for (int i = 0; i < filenames.size(); i++) {
      file_type = FileName.ParseFileType(filenames.get(i));
      file_num = FileName.ParseFileNumber(filenames.get(i), file_type);
      boolean keep = true;
      switch (file_type) {
      case FileName.LOG_FILE:
        keep = file_num >= version_set.LogNumber();
        break;
      case FileName.DESC_FILE:
        keep = file_num >= version_set.ManifestFileNumber();
        break;
      case FileName.TABLE_FILE:
        keep = live.contains(file_num);
        break;
      case FileName.TMP_FILE:
        keep = live.contains(file_num);
        break;
      default:
        keep = true;
      }
      if (!keep) {
        if (file_type == FileName.TABLE_FILE)
          table_cache.Evict(file_num);
        new File(dbname + "/" + filenames.get(i)).delete();
      }
    }
  }

  public void MaybeScheduleCompaction() throws IOException, BadFormatException, DecodeFailedException {
    if (bg_compaction_scheduled) {
      // already scheduled
    } else if (shutting_down) {
      // DB is being deleted; no more background compactions
    } else if (imm == null && manual_compaction == null && !version_set.NeedCompaction()) {
      // No work need to be done
    } else {
      bg_compaction_scheduled = true;
      BackgroundCompaction();
      MaybeScheduleCompaction();
    }
  }
  
  private void BackgroundCompaction() throws IOException, BadFormatException, DecodeFailedException {
    if (imm != null) {
      CompactMemTable();
    }
    
    Compaction c = null;
    boolean is_manual = (manual_compaction != null);
    InternalKey manual_end = null;
    
    if (is_manual) {
      ManualCompaction m = manual_compaction;
      c = version_set.CompactRange(m.level, m.begin, m.end);
      m.done = c == null;
      if (c != null) {
        manual_end = c.Input(0, c.NumInputFiles(0) - 1).largest;
      }
    } else {
      c = version_set.PickCompaction();
    }
    
     if (c == null) {
       // nothing to do
     } else if (!is_manual && c.IsTrivialMove()) {
       // move file to next level
       FileMetaData file = c.Input(0,  0);
       c.Edit().DeleteFile(c.Level(), file.number);
       c.Edit().AddFile(c.Level() + 1, file.number, file.file_size, file.smallest, file.largest);
       version_set.LogAndApply(c.Edit());
       
     } else {
       CompactionState compact = new CompactionState(c);
       DoCompactionWork(compact);
       CleanupCompaction(compact);
       c.ReleaseInputs();
       DeleteObsoleteFiles();
     }
     
     if (is_manual) {
       ManualCompaction m = manual_compaction;
       m.done = true;
     }
  }
  
  private void CleanupCompaction(CompactionState compact) {
    if (compact.builder != null) {
      compact.builder.Abandon();
    } else {
      
    }
    for (int i = 0; i < compact.outputs.size(); i++) {
      pending_outputs.remove(compact.outputs.get(i).number);
    }
  }
  
  private void OpenCompactionOutputFile(CompactionState compact) throws FileNotFoundException {
    long file_number = version_set.NewFileNumber();
    pending_outputs.add(file_number);
    CompactionState.Output out = new CompactionState.Output();
    out.number = file_number;
    out.smallest = null;
    out.largest = null;
    compact.outputs.add(out);
    
    String table_name = FileName.TableFileName(dbname, file_number);
    compact.outfile = new DataOutputStream(new FileOutputStream(table_name));
    compact.builder = new TableBuilder(options, compact.outfile);
  }
  
  private void FinishCompactionOutputFile(CompactionState compact) throws IOException {
    long output_number = compact.current_output().number;
    long current_entries = compact.builder.NumEntries();
    compact.builder.Finish();
    
    long current_bytes = compact.builder.FileSize();
    compact.current_output().file_size = (int) current_bytes;
    compact.total_bytes += current_bytes;
    compact.builder = null;
    
    compact.outfile.flush();
    compact.outfile.close();
    compact.outfile = null;
  }
  
  private void InstallCompactionResults(CompactionState compact) throws BadFormatException, IOException {
    compact.compaction.AddInputDeletions(compact.compaction.Edit());
    int level = compact.compaction.Level();
    for (int i = 0; i < compact.outputs.size(); i++) {
      CompactionState.Output out = compact.outputs.get(i);
      compact.compaction.Edit().AddFile(level + 1, out.number, out.file_size, out.smallest, out.largest);
    }
    version_set.LogAndApply(compact.compaction.Edit());
  }
  
  private void DoCompactionWork(CompactionState compact) throws IOException, DecodeFailedException, BadFormatException {
    if (snapshots.isEmpty()) {
      compact.smallest_snapshot = version_set.lastSequence();
    } else {
      compact.smallest_snapshot = snapshots.getFirst();
    }
    
    TableMergeIterator input = version_set.MakeInputIterator(compact.compaction);
    input.SeekToFirst();
    ParsedInternalKey ikey = null;
    Slice current_user_key = null;
    boolean has_current_user_key = false;
    long last_sequence_for_key = Settings.MaxSequenceNumber;
    for (; input.Valid(); ) {
      if (imm != null) {
        CompactMemTable();
      }
      Slice key = input.Key();
      if (compact.compaction.ShouldStopBefore(key) && compact.builder != null) {
        FinishCompactionOutputFile(compact);
      }
      
      boolean drop = false;
      ikey = new ParsedInternalKey().ParseInternalKey(key);
      if (ikey == null) {
        current_user_key = null;
        has_current_user_key = false;
        last_sequence_for_key = Settings.MaxSequenceNumber;
      } else {
        if (!has_current_user_key || internal_comparator.GetUserComparator().Compare(ikey.user_key, current_user_key) != 0) {
          current_user_key = ikey.user_key;
          has_current_user_key = true;
          last_sequence_for_key = Settings.MaxSequenceNumber;
        }
        
        if (last_sequence_for_key <= compact.smallest_snapshot) {
          drop = true;
        } else if (ikey.type == Settings.OP_TYPE_DELETE &&
                   ikey.sequence <= compact.smallest_snapshot &&
                   compact.compaction.IsBaseLevelForKey(ikey.user_key)) {
          drop = true;
        }
        last_sequence_for_key = ikey.sequence;
      }
      if (!drop) {
        if (compact.builder == null) {
          OpenCompactionOutputFile(compact);
        }
        if (compact.builder.NumEntries() == 0) {
          compact.current_output().smallest.DecodeFrom(key.GetData(), key.GetOffset(), key.GetLength());
        }
        compact.current_output().largest.DecodeFrom(key.GetData(), key.GetOffset(), key.GetLength());
        compact.builder.Add(key,  input.value());
        
        if (compact.builder.FileSize() >= compact.compaction.MaxOutputFileSize()) {
          FinishCompactionOutputFile(compact);
        }
      }
      input.Next();
    }
    
    if (compact.builder != null) {
      FinishCompactionOutputFile(compact);
    }
    
    InstallCompactionResults(compact);
  }

  private boolean CheckDBOptions() throws IOException {
    if (!Util.CreateDir(dbname))
      return false;
    if (!Util.FileExists(FileName.CurrentFileName(dbname))) {
      if (options.creat_if_missing) {
        InitializeDB();
        return true;
      } else {
        return false;
      }
    } else if (options.error_if_existing) {
      return false;
    }
    return true;
  }

  private void InitializeDB() throws IOException {
    VersionEdit edit = new VersionEdit();
    String comparator_name = internal_comparator.Name();
    edit.SetComparatorName(new Slice(comparator_name.getBytes()));
    edit.SetLogNumber(0);
    edit.SetNextFile(2);
    edit.SetLastSequence(0);

    String manifest = FileName.DescriptorFileName(dbname, 1);
    DataOutputStream writer = new DataOutputStream(new FileOutputStream(
        manifest));
    LogWriter log_writer = new LogWriter(writer);
    Slice encoded_edit = edit.EncodeTo();
    log_writer.AddRecord(encoded_edit);
    writer.close();

    FileName.SetCurrentFile(dbname, 1);
  }

  private long RecoverLogFile(long log_number, VersionEdit edit)
      throws IOException, BadFormatException {
    String fname = FileName.LogFileName(dbname, log_number);
    DataInputStream reader = new DataInputStream(new FileInputStream(fname));
    LogReader log_reader = new LogReader(reader, true, 0);
    byte[] record = null;
    WriteBatch batch = new WriteBatch(0);
    long max_seq = 0;

    while (true) {
      record = log_reader.ReadRecord();
      if (record == null)
        break;
      if (record.length < 12)
        throw new BadFormatException("record size too small");
      batch.SetContents(new Slice(record));

      if (mem == null) {
        mem = new MemTable(internal_comparator);
      }
      batch.InsertInto(mem);
      long last_seq = batch.Sequence() + batch.Count() - 1;
      if (last_seq > max_seq)
        max_seq = last_seq;

      if (mem.ApproximateMemoryUsage() > options.write_buffer_size) {
        WriteLevel0Table(mem, edit, null);
      }
      mem = null;
    }
    ;

    if (mem != null)
      WriteLevel0Table(mem, edit, null);
    return max_seq;
  }

  private void WriteLevel0Table(MemTable mem, VersionEdit edit, Version base) throws IOException {
    FileMetaData file_meta = new FileMetaData();
    file_meta.number = version_set.NewFileNumber();
    pending_outputs.add(file_meta.number);
    MemIterator mem_iter = mem.Iterator();
    TableBuilder.BuildTable(dbname, options, mem_iter, file_meta);
    pending_outputs.remove(file_meta.number);

    int level = 0;
    if (file_meta.file_size > 0) {
      Slice min_user_key = file_meta.smallest.GetUserKey();
      Slice max_user_key = file_meta.largest.GetUserKey();
      if (base != null)
        level = base.PickLevelForMemTableOutput(min_user_key, max_user_key);
      edit.AddFile(level, file_meta.number, file_meta.file_size,
          file_meta.smallest, file_meta.largest);
    }
    
    CompactionStats stat = new CompactionStats();
    stat.micros = 0;
    stat.bytes_written = file_meta.file_size;
    stats[level].Add(stat);
  }
  
  private void TEST_CompactRange(int level, Slice begin, Slice end) throws IOException, BadFormatException, DecodeFailedException {
    ManualCompaction manual = new ManualCompaction();
    
    manual.level = level;
    manual.done = false;
    
    if (begin == null) {
      manual.begin = null;
    } else {
      manual.begin = new InternalKey(begin, Settings.MaxSequenceNumber, Settings.OP_TYPE_SEEK);
    }
    
    if (end == null) {
      manual.end = null;
    } else {
      manual.end = new InternalKey(end, 0, (byte) 0);
    }
    
    while (!manual.done && !shutting_down) {
      if (manual_compaction == null) {
        manual_compaction = manual;
        MaybeScheduleCompaction();
      } else {
        
      }
    }
    
    if (manual_compaction == manual) {
      manual_compaction = null;
    }
  }
  
  private void TEST_CompactMemTable() throws IOException, BadFormatException, DecodeFailedException {
    Write(new WriteOptions(), null);
  }

  private void MakeRoomForWrite(boolean force) throws IOException, BadFormatException, DecodeFailedException {
    boolean allow_delay = !force;
    while (true) {
      if (allow_delay && version_set.NumLevelFiles(0) >= Settings.L0_SLoWDOWN_WRITES_TIGGER) {
        // we are getting close to hitting a hard limit on the number of L0 files. Rather than delaying
        // a single write by several seconds when we hit the hard limit, start delaying each individual
        // write by 1ms to reduce latency variance. Also, this delay hands over some cpu to the compaction
        // thread in case it's sharing the same core as the writer.
        allow_delay = false;
      } else if (!force && mem.ApproximateMemoryUsage() <= options.write_buffer_size) {
        break;
      } else if (imm != null) {
        
      } else if (version_set.NumLevelFiles(0) >= Settings.L0_STOP_WRITES_TRIGGER) {
        
      } else {
        long new_log_number = version_set.NewFileNumber();
        String file_name = FileName.LogFileName(dbname, new_log_number);
        DataOutputStream writer = new DataOutputStream(new FileOutputStream(file_name));
        this.log_file = writer;
        this.log_num = new_log_number;
        this.log = new LogWriter(writer);
        
        imm = mem;
        mem = new MemTable(internal_comparator);
        force = false;
        MaybeScheduleCompaction();
      }
    }
  }

  /**
   * Writer list must be non-empty, First writer must have a non-NULL batch
   */
  private Writer BuildBatchGroup(WriteBatch batch) {
    if (writers.isEmpty()) return null;
    int size = 0;
    Iterator<Writer> iter = writers.iterator();
    Writer last = null;
    
    int max_size = 1 << 20;
    if (writers.get(0).batch.ByteSize() <= (128 << 10))
      max_size = writers.get(0).batch.ByteSize() + (128 << 10);
    
    while (iter.hasNext()) {
      last = iter.next();
      if (last.batch == null) continue;
      batch.Append(last.batch);
      size += last.batch.ByteSize();
      if (size > max_size) break;
    }
    return last;
  }

  @Override
  public Slice Get(ReadOptions options, Slice key) throws IOException, DecodeFailedException, BadFormatException {
    long snapshot = 0;
    if (options.snapshot != 0) {
      snapshot = options.snapshot;
    } else {
      snapshot = version_set.lastSequence();
    }
    MemTable mem_tmp = mem;
    MemTable imm_tmp = imm;
    Version current = version_set.Current();
    boolean have_stat_update = false;
    {
      LookupKey lkey = new LookupKey(key, snapshot);
      Slice val = mem.Get(lkey);
      if (val != null) {
        
      } else if (imm != null) {
        val = imm.Get(lkey);
      }
      if (val != null) {
        val = current.Get(options, lkey);
        have_stat_update = true;
      }
    }
    
    if (have_stat_update) {
      MaybeScheduleCompaction();
    }
    return null;
  }

  @Override
  public DBIterator NewIterator(ReadOptions options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long NewFileNumber() {
    return version_set.NewFileNumber();
  }

  @Override
  public SnapShot GetSnapshot() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean ReleaseSnapshot(SnapShot snapshot) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String GetProperty(Slice property) {
    Slice in = property;
    Slice prefix = new Slice("leveldb.".getBytes());
    throw new RuntimeException("Method not implemented yet");
  }

  @Override
  public long[] GetApproximateSizes(Range[] range, int n) throws IOException, DecodeFailedException, BadFormatException {
    Version v= version_set.Current();
    long[] sizes = new long[n];
    for (int i = 0; i < n; i++) {
      InternalKey k1 = new InternalKey(range[i].start, Settings.MaxSequenceNumber, Settings.OP_TYPE_SEEK);
      InternalKey k2 = new InternalKey(range[i].limit, Settings.MaxSequenceNumber, Settings.OP_TYPE_SEEK);
      long start = version_set.ApproximateOffsetOf(v, k1);
      long limit = version_set.ApproximateOffsetOf(v, k2);
      sizes[i] = (limit >= start ? limit - start : 0);
    }
    return sizes;
  }
  
  private void CompactMemTable() throws IOException, BadFormatException {
    VersionEdit edit = new VersionEdit();
    Version base = version_set.Current();
    WriteLevel0Table(imm, edit, base);
    edit.SetLogNumber(log_num);
    version_set.LogAndApply(edit);
    
    imm = null;
    DeleteObsoleteFiles();
  }

  @Override
  public boolean CompactRange(Slice begin, Slice end) throws IOException, BadFormatException, DecodeFailedException {
    int max_level_with_files = 1;
    {
      Version base = version_set.Current();
      for (int level = 1; level < Settings.NUM_LEVELS; level++) {
        if (base.OverlapInLevel(level, begin, end)) max_level_with_files = level;
      }
    }
    
    TEST_CompactMemTable();
    for (int level = 0; level < max_level_with_files; level++) {
      TEST_CompactRange(level, begin, end);
    }
    return true;
  }  
}
