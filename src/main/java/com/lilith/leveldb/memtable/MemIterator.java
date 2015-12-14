package com.lilith.leveldb.memtable;


import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.memtable.MemTable.KeyComparator;

/**
 * A wrapper class of SkipList.Iterator for easy use.
 */
public class MemIterator {
  SkipList<Slice, KeyComparator>.Iterator mem_iter = null;
  
  public MemIterator(SkipList<Slice, KeyComparator>.Iterator iter) {
    mem_iter = iter;
  }
  
  public boolean Valide() { return mem_iter.Valid(); }
  
  public void Seek(Slice key) { mem_iter.Seek(key); }
  
  public void SeekToFirst() { mem_iter.SeekToFirst(); }
  
  public void SeekToLast() { mem_iter.SeekToLast(); }
  
  public void Next() { mem_iter.Next(); }
 
  public void  Prev() { mem_iter.Prev(); }
  
  public Slice Key() { return Slice.GetLengthPrefix(mem_iter.Key()); }
  
  public Slice Value() {
    Slice key_slice = Key();
    return Slice.GetLengthPrefix(key_slice.GetData(), key_slice.GetLength() + key_slice.GetOffset());
  }
}
