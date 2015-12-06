package com.lilith.leveldb.api;

import com.lilith.leveldb.impl.SnapShot;
import com.lilith.leveldb.impl.WriteBatch;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.WriteOptions;

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
   */
  public static LevelDB Open(Options options, String name) {
    return null;
  }
  
  /**
   * Set the database entry for "key" to "value". Return true on success.
   */
  public abstract boolean Put(WriteOptions options, Slice key, Slice value);
  
  /**
   * Apply the specified updates to the database. Return true on success.
   */
  public abstract boolean Delete(WriteOptions options, Slice key);
  
  /**
   * Apply the specified updates to the database. Return true on success.
   */
  public abstract boolean Write(WriteOptions options, WriteBatch updates);
  
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
   * 
   */
  public abstract String GetProperty(Slice property);
}
