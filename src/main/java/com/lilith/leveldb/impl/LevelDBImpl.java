package com.lilith.leveldb.impl;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import com.lilith.leveldb.api.DBIterator;
import com.lilith.leveldb.api.LevelDB;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.log.LogWriter;
import com.lilith.leveldb.util.FileName;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.Range;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.Util;
import com.lilith.leveldb.util.WriteOptions;
import com.lilith.leveldb.version.Version;
import com.lilith.leveldb.version.VersionSet;
import com.lilith.leveldb.version.VersionEdit;

public class LevelDBImpl extends LevelDB {
  
  public Options options = null;
  public String dbname = null;
  
  public VersionEdit version_edit = null;
  public VersionSet  version_set  = null;
  
  public DataOutputStream log_file = null;
  public long log_num = 0;
  public LogWriter log = null;
  
  
  public LevelDBImpl(Options options, String dbname) {
    this.options = options;
    this.dbname = dbname;
  }

  @Override
  public boolean Put(WriteOptions options, Slice key, Slice value) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean Delete(WriteOptions options, Slice key) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean Write(WriteOptions options, WriteBatch updates) {
    // TODO Auto-generated method stub
    return false;
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
  
  /**
   * Recover the descriptor from persistent storage. May do a significant
   * amount of work to recover recently logged updates. Any changes to
   * be made to the descriptor are added to edit.
   */
  private VersionEdit Recover() {
    if (!Util.CreateDir(dbname)) return null;
    if (!Util.FileExists(FileName.CurrentFileName(dbname))) {
      if (options.creat_if_missing) {
        
      } else {
        return null;
      }
    } else if (options.error_if_existing) {
      return null;
    }
    
    try {
      version_set.Recover();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    } catch (BadFormatException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
    
    long seq_num = 0;
    // Recover from all newer log files than the ones named in the descriptor
    // (new log files may have been added by the previous incarnation without 
    // registering them in descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay attention to it
    // in case we are recovering a database produced by an older version of leveldb
    long min_log = version_set.LogNumber();
    long prev_log = version_set.PrevLogNumber();
    ArrayList<String> filenames = new ArrayList<String>();
    Util.GetChildren(dbname, filenames);
    
    HashSet<Long> expected = new HashSet<Long>();
    version_set.AddliveFiles(expected);
    
    // Parse all log files.
    ArrayList<Long> logs;
    for (int i = 0; i < filenames.size(); i++) {
      long file_num = FileName.ParseFileNumber(filenames.get(i));
      int file_type = FileName.ParseFileType(filenames.get(i));
      if (file_type == FileName.LOG_FILE && (file_num > min_log))
        logs.add(file_num);
    }
    
    if (!expected.isEmpty()) throw new Exception("missing files");
    
    
    // Recover in the order in which the logs were generated.
    ArrayList<Long> sorted_logs = logs.sort(c);
    for (int i = 0; i < logs.size(); i++) {
      RecoverLogFile(logs[i], edit, max_seq);
      version_set.MarkFileNumberUsed(sorted_logs.get(i));
    }
    
    if (version_set.lastSequence() < max_seq) 
      version_set.SetLastSequence(max_seq);
    
    return null;
  }
  
  
  private void InitializeDB() throws IOException {
    VersionEdit edit = new VersionEdit();
    edit.SetComparatorName(InternalKeyComparator.GetName());
    edit.SetLogNumber(0);
    edit.SetNextFile(2);
    edit.SetLastSequence(0);
    
    String manifest = FileName.DescriptorFileName(dbname, 1);
    DataOutputStream writer = new DataOutputStream(new FileOutputStream(manifest));
    LogWriter log_writer = new LogWriter(writer);
    Slice encoded_edit = edit.EncodeTo();
    log_writer.AddRecord(encoded_edit);
    writer.close();
    
    FileName.SetCurrentFile(dbname, 1);
  }
  
  private boolean RecoverLogFile(long log_number, VersionEdit edit, long max_sequence) {
    return false;
  }
  
  /**
   * Delete any unneeded files and stale in-memory entries
   */
  private void DeleteObsoleteFiles() {
    
  }
  
  /**
   * Compact the in-memory write buffer to disk. Switches to a new log-file/memtable and writes
   * a new descriptor iff successful.
   */
  private void CompactMemTable() {
    
  }
  
  private void WriteLevel0Table(MemTable mem, VersionEdit edit, Version base) {
    
  }
  
  private void MakeRoomForWrite(boolean force) {
    
  }
  
  private WriteBatch BuildBatchGroup(DataOutputStream writer) {
    return null;
  }
  
  private void MayBeScheduleCompaction() {
    
  }
  
  private void DoCompactionWork() {
    
  }
  
  private void CleanupCompaction() {
    
  }
  
  private void OpenCompactionOutputFile() {
    
  }
  
  private void FinishCompactionOutputFile() {
    
  }
  
  private void InstallCompactionResults() {
     
  }

}
