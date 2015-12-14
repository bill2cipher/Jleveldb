package com.lilith.leveldb.table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Vector;

import com.lilith.leveldb.api.Slice;

public class LRUCache<Key, Value> implements Cache<Key, Value> {
  
  /**
   * An entry is a variable length heap-allocated class. Entries are kept in a circular doubly linked list ordered by access time.
   */
  private class Entry {
    public Value value;
    public Key key;

    public long charge;
    public int hash;         // hash of key, used for fast sharding and comparisons
  }
  
  private class CacheTable {
    private LinkedList<Entry>[] entries = null;
    private int elems = 0;
    
    public CacheTable() {
      elems = 0;
    }
    
    public Entry Lookup(Key key, int hash) {
      return FindPointer(key, hash);
    }
    
    public Entry Insert(Entry entry) {
      LinkedList<Entry> entries = FindPointer(entry.key, entry.hash);
      if (entries == null) {
        elems++;
        if (elems > length) Resize();
      }
    }
    
    private Entry FindPointer(Key key, int hash) {
      LinkedList<Entry> val = entries.get(hash & (entries.size() - 1));
    }
    
    private void Resize() {
      int new_length = 1;
      while (new_length < entries.capacity()) new_length *= 2;
      Vector<LinkedList<Entry>> new_entries = new Vector<LinkedList<Entry>>(new_length);
      new_entries.setSize(new_length);

      for (int i = 0; i < entries.size(); i++) {
        LinkedList<Entry> val_list = entries.get(i);
        if(val_list == null) continue;
        
        Iterator<Entry> iter = val_list.iterator();
        while (iter.hasNext()) {
          Entry entry = iter.next();
          LinkedList<Entry> new_pos = new_entries.get(entry.hash & (new_length - 1));
          if (new_pos == null) {
            new_pos = new LinkedList<Entry>();
            new_entries.setElementAt(new_pos, entry.hash & (new_length - 1));
          }
          new_pos.add(entry);
        }
      }
    }
  }
  
  
  private LinkedList<Key> lru = null;
  
  public LRUCache() {
    entries = 
  }

  public void Insert(Slice key, Slice value, int charge) {
  }

  public Slice Lookup(Slice key) {
    // TODO Auto-generated method stub
    return null;
  }

  public void Erase(Slice key) {
    // TODO Auto-generated method stub
    
  }

  public long NewId() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  private Entry FindEntry(Slice key, int hash) {
    LinkedList<Entry> values = entries.get(hash);
    if (values == null) return null;
    Iterator<Entry> iter = values.iterator();
    while (iter.hasNext()) {
      Entry entry = iter.next();
      if (entry.key.compareTo(key) == 0) return entry;
    }
    return null;
  }

  public void Insert(Key key, Value value, Comparator<Key> comp, int charge) {
    // TODO Auto-generated method stub
    
  }

  public Value Lookup(Key key) {
    // TODO Auto-generated method stub
    return null;
  }

  public void Erase(Key key) {
    // TODO Auto-generated method stub
    
  }

}
