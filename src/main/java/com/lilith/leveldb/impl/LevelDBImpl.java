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
import java.util.Set;

import com.lilith.leveldb.api.DBIterator;
import com.lilith.leveldb.api.LevelDB;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.api.SliceComparator;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.log.LogReader;
import com.lilith.leveldb.log.LogWriter;
import com.lilith.leveldb.memtable.MemIterator;
import com.lilith.leveldb.memtable.MemTable;
import com.lilith.leveldb.table.TableBuilder;
import com.lilith.leveldb.table.TableCache;
import com.lilith.leveldb.util.FileLocker;
import com.lilith.leveldb.util.FileName;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.Range;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.Util;
import com.lilith.leveldb.util.WriteOptions;
import com.lilith.leveldb.version.FileMetaData;
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

  private class Writer {
    public WriteBatch batch = null;
    public boolean sync = false;
    public boolean done = false;
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
  }

  @Override
  public void Put(WriteOptions options, Slice key, Slice value) throws IOException, BadFormatException {
    WriteBatch batch = new WriteBatch(0);
    batch.Put(key, value);
    Write(options, batch);
  }

  @Override
  public void Delete(WriteOptions options, Slice key) throws IOException, BadFormatException {
    WriteBatch batch = new WriteBatch(0);
    batch.Delete(key);
    Write(options, batch);
  }

  @Override
  public void Write(WriteOptions options, WriteBatch updates) throws IOException, BadFormatException {
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

  public void MaybeScheduleCompaction() {
    if (bg_compaction_scheduled) {
      // already scheduled
    } else if (shutting_down) {
      // DB is being deleted; no more background compactions
    } else {
      bg_compaction_scheduled = true;
      // do work here
    }
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
  }

  private void MakeRoomForWrite(boolean force) {
    
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
  public Slice Get(ReadOptions options, Slice key) {
    // TODO Auto-generated method stub
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int[] GetApproximateSizes(Range[] range, int n) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean CompactRange(Slice begin, Slice end) {
    // TODO Auto-generated method stub
    return false;
  }
}
