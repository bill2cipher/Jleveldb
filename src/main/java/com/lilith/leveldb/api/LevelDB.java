package com.lilith.leveldb.api;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.impl.LevelDBImpl;
import com.lilith.leveldb.impl.SnapShot;
import com.lilith.leveldb.impl.WriteBatch;
import com.lilith.leveldb.log.LogWriter;
import com.lilith.leveldb.util.FileName;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.Range;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.WriteOptions;
import com.lilith.leveldb.version.VersionEdit;

/**
 * A leveldb is a persistent ordered map from keys to values.
 * A leveldb is safe for concurrent access from multiple threads without
 * any external synchronization.
 *
 */
public abstract class LevelDB {
  /**
   * Open the database with the specified db name.
   * @param options
   * @param name
   * @return
   * @throws BadFormatException 
   * @throws IOException 
   */
  public static LevelDB Open(Options options, String dbname) throws IOException, BadFormatException {
    LevelDBImpl impl = new LevelDBImpl(options, dbname);
    VersionEdit version_edit = impl.Recover();
    if (version_edit != null) {
      long log_number = impl.NewFileNumber();
      DataOutputStream log_writer = new DataOutputStream(new FileOutputStream(FileName.LogFileName(dbname, log_number)));
      version_edit.SetLogNumber(log_number);
      
      impl.log_file = log_writer;
      impl.log_num = log_number;
      impl.log = new LogWriter(log_writer);
      impl.version_set.LogAndApply(version_edit);
      
      impl.DeleteObsoleteFiles();
      impl.MaybeScheduleCompaction();
    }
    return impl;
  }
  
  /**
   * Destroy the contents of the specified database.
   */
  public static boolean DestroyDB(Options options, String name) {
    return false;
  }
  
  /**
   * If a db cannot be opened, you may attempt to call this method to
   * resurrect as much of the contents of the database as possible.
   * Some data may be lost, so be careful when calling this function
   * on a database that contains important information.
   */
  public static boolean RepairDB(Options options, String name) {
    return false;
  }
  
  /**
   * Set the database entry for "key" to "value". Return true on success.
   * @throws BadFormatException 
   * @throws IOException 
   */
  public abstract void Put(WriteOptions options, Slice key, Slice value) throws IOException, BadFormatException;
  
  /**
   * Apply the specified updates to the database. Return true on success.
   * @throws BadFormatException 
   * @throws IOException 
   */
  public abstract void Delete(WriteOptions options, Slice key) throws IOException, BadFormatException;
  
  /**
   * Apply the specified updates to the database. Return true on success.
   * @throws IOException 
   * @throws BadFormatException 
   */
  public abstract void Write(WriteOptions options, WriteBatch updates) throws IOException, BadFormatException;
  
  /**
   * If the database contains an entry for key store the corresponding value
   * in value and return ok.
   * 
   * If there is no entry for key leave return null.
   * @return
   */
  public abstract Slice Get(ReadOptions options, Slice key);
  
  /**
   * Return an iterator over the contents of the database. The result of 
   * NewIterator() is initially invalid (caller must call one of the seek methods
   * on the iterator before using it).
   */
  public abstract DBIterator NewIterator(ReadOptions options);
  
  /**
   * Return the next file number for table.
   */
  public abstract long NewFileNumber();
  
  /**
   * Return a handle to the current DB. Iterators created with this handle will all
   * observe a stable snapshot of the current DB state. The caller must call
   * ReleaseSnapshot(result) when the snapshot is no longer needed.
   */
  public abstract SnapShot GetSnapshot();
  
  /**
   * Release a previous acquired snapshot. The caller must not use snapshot after
   * this call.
   */
  public abstract boolean ReleaseSnapshot(SnapShot snapshot);
  
  /**
   * DB implementation can export properties about their state via this method.
   * If "property" is a valid property understood by this DB implementation, return
   * the value. Otherwise return null.
   * 
   * Valid property names include:
   * "leveldb.num-files-at-level<N>" - return the number of files at level <N>
   * "leveldb.stats" - return a multi-line string that describes statistics about the
   *                   internal operations of the DB
   * "leveldb.sstables" - returns a multi-line string that describes all of the sstables
   *                      that make up the db contents.
   */
  public abstract String GetProperty(Slice property);
  
  /**
   * For each i in [0, n - 1], return an integer, the approximate file system space
   * used by keys in range[i].start range[i].limit.
   * 
   * Note that the returned size measure file system space usage, so if the user data
   * compresses by a factor of ten, the returned sizes will be one-tenth the size of
   * the corresponding user data size.
   */
  public abstract int[] GetApproximateSizes(Range[] range, int n);
  
  /**
   * Compact the underlying storage for the key range [begin, end]. In particular, deleted
   * and overwritten versions are discarded, and the data is rearranged to reduce the cost
   * of operations needed to access the data. This operation should typically only be invoked
   * by users who understand the underlying implementation.
   * 
   * begin == null is treated as a key before all keys in the database.
   * end == null is treated as a key after all keys in the database.
   * Therefore the following call will compact the entire database:
   * CompactRange(null, null).
   */
  public abstract boolean CompactRange(Slice begin, Slice end);
}
