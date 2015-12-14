package com.lilith.leveldb.table;

/**
 * A Cache is an interface that maps key to values. It has internal synchronization and may be safely
 * accessed concurrently from multiple threads. It may automatically evict entries to make room for
 * new entries. Values have a specified charge against the cache capacity. For example, a cache where
 * the values are variable length strings, may use the length of the strings as the charge for the string.
 * 
 * A builtin cache implementation with a least-recently-used eviction policy is provided. Clients may use
 * their own implementation if they want something more sophisticated.
 * 
 * @author Administrator
 */
public interface Cache <Key, Value> {
  
  /**
   * Insert a mapping from key->value into the cache and assign it the specified charge against the total cache capacity.
   */
  public void Insert(Key key, Value value, int charge, int hash);
  
  /**
   * If the cache has no mapping for key, returns null.
   * Else return an slice representing the mapping.
   */
  public Value Lookup(Key key, int hash);
  
  /**
   * If the cache contains entry for key, erase it. Not that the underlying entry
   * will be kept around until all existing reference to it has been released.
   */
  public void Erase(Key key, int hash);
  
  /**
   * Return a new numeric id. May be used by multiple clients who are sharing the same cache
   * to partition the key space. Typically the client will allocate a new id at startup and
   * prepend the id to its cache keys.
   */
  public long NewId();
}
