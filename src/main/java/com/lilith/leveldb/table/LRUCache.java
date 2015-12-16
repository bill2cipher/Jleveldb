package com.lilith.leveldb.table;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

public class LRUCache<Key extends Comparable<Key>, Value> implements Cache<Key, Value> {
  
  /**
   * An entry is a variable length heap-allocated class. Entries are kept in a circular doubly linked list ordered by access time.
   */
  private class Entry {
    public Value value;
    public Key key;

    public long charge;
    public int hash;         // hash of key, used for fast sharding and comparisons
    
    public Entry next;
    public Entry prev;
  }
  
  private class CacheTable {
    private LinkedList<Entry>[] entries = null;
    private int elems = 0;
    
    public CacheTable() {
      elems = 0;
    }
    
    public Entry Lookup(Key key, int hash) {
      LinkedList<Entry> val = entries[hash & (entries.length - 1)];
      if (val == null) return null;
      Iterator<Entry> iter = val.iterator();
      while (iter.hasNext()) {
        Entry entry = iter.next();
        if (entry.key.compareTo(key) == 0) return entry;
      }
      return null;
    }
    
    public Entry Insert(Entry entry) {
      LinkedList<Entry> val = entries[entry.hash & (entries.length - 1)];
      Entry old = Remove(entry.key, entry.hash);
      if (val == null) {
        val = new LinkedList<Entry>();
        entries[entry.hash & (entries.length - 1)] = val; 
      }
      val.add(entry);
      elems++;
      if (elems > entries.length) Resize();
      return old;
    }
    
    public Entry Remove(Key key, int hash) {
      LinkedList<Entry> val = entries[hash & (entries.length - 1)];
      if (val == null) return null;
      Iterator<Entry> iter = val.iterator();
      while (iter.hasNext()) {
        Entry entry = iter.next();
        if (entry.key.compareTo(key) == 0) {
          iter.remove();
          return entry;
        }
      }
      return null;
    }
    
    private void Resize() {
      int new_length = 4;
      while (new_length < entries.length) new_length *= 2;
      LinkedList<Entry>[] new_entries = (LinkedList<LRUCache<Key, Value>.Entry>[]) new LinkedList[new_length];

      for (int i = 0; i < entries.length; i++) {
        LinkedList<Entry> val_list = entries[i];
        if(val_list == null) continue;
        
        Iterator<Entry> iter = val_list.iterator();
        while (iter.hasNext()) {
          Entry entry = iter.next();
          LinkedList<Entry> new_val = new_entries[entry.hash & (new_length - 1)];
          if (new_val == null) {
            new_val = new LinkedList<Entry>();
            new_entries[entry.hash & (new_length - 1)] = new_val;
          }
          new_val.add(entry);
        }
      }
      entries = new_entries;
    }
  }
  
  // lru.prev is newest entry, lru.next is oldest entry  
  private Entry lru = null;
  private int capacity = 0;
  private int usage = 0;
  private CacheTable table = null;
  private AtomicLong cache_id;
  
  
  public LRUCache() {
    lru = new Entry();
    lru.next = lru;
    lru.prev = lru;
    capacity = 0;
    usage = 0;
    table = new CacheTable();
    cache_id = new AtomicLong(0);
  }
  
  private void LRURemove(Entry entry) {
    entry.next.prev = entry.prev;
    entry.prev.next = entry.next;
  }
  
  private void LRUAppend(Entry entry) {
    entry.next = lru;
    entry.prev = lru.prev;
    entry.prev.next = entry;
    entry.next.prev = entry;
  }
  
  
  private Entry NewEntry(Key key, Value value, int charge, int hash) {
    Entry entry = new Entry();
    entry.key = key;
    entry.value = value;
    entry.charge = charge;
    entry.hash = hash;
    return entry;
  }


  public Value Lookup(Key key, int hash) {
    Entry entry = table.Lookup(key, hash);
    if (entry == null) return null;
    LRURemove(entry);
    LRUAppend(entry);
    return entry.value;
  }
  

  public void Insert(Key key, Value value, int charge, int hash) {
    Entry entry = NewEntry(key, value, charge, hash);
    LRUAppend(entry);
    usage += charge;
    
    Entry old = table.Insert(entry);
    if (old != null) {
      LRURemove(old);
      usage -= old.charge;
    }
    
    while (usage > capacity && lru.next != lru) {
      old = lru.next;
      LRURemove(old);
      table.Remove(old.key, old.hash);
      usage -= old.charge;
    }
  }

  public void Erase(Key key, int hash) {
    Entry entry = table.Remove(key, hash);
    if (entry != null) {
      usage -= entry.charge;
      LRURemove(entry);
    }
  }

  public long NewId() {
    return cache_id.addAndGet(1);
  }
  
  public void SetCapacity(int capacity) {
    this.capacity = capacity;
  }
}
