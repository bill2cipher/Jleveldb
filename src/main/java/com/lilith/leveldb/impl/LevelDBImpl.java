package com.lilith.leveldb.impl;

import java.io.DataOutputStream;

import com.lilith.leveldb.api.DBIterator;
import com.lilith.leveldb.api.LevelDB;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.Range;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.WriteOptions;
import com.lilith.leveldb.version.Version;
import com.lilith.leveldb.version.VersionEdit;

public class LevelDBImpl extends LevelDB {
  private Options options = null;
  private String dbname = null;
  private VersionEdit edit = null;
  private DataOutputStream log_writer = null;
  
  
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
  private void Recover(VersionEdit edit) {
    
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
