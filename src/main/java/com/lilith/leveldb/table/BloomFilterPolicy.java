package com.lilith.leveldb.table;

import java.util.ArrayList;
import java.util.Iterator;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.Settings;
import com.lilith.leveldb.util.Util;

public class BloomFilterPolicy extends FilterPolicy {
  
  private Slice name = null;
  private int bits_per_key = 0;
  private int k = 0;
  
  public static int BloomHash(Slice key) {
    return Util.Hash(key.GetData(), key.GetOffset(), key.GetLength(), Settings.HashSeed);
  }
  
  public BloomFilterPolicy(int bits_per_key) {
    name = new Slice("leveldb.BuiltinBoolmFilter".getBytes());
    this.bits_per_key = bits_per_key;
    k = (int) (bits_per_key * 0.69);
    if (k < 1) k = 1;
    if (k > 30) k = 30;
  }

  @Override
  public Slice Name() {
    return name;
  }

  @Override
  public Slice CreateFilter(ArrayList<Slice> keys) {
    int bits = keys.size() * bits_per_key;
    int bytes = (bits + 7) / 8;
    bits = bytes * 8;
    byte[] buffer = new byte[bytes];
    buffer[0] =(byte) k;

    Iterator<Slice> iter = keys.iterator();
    while (iter.hasNext()) {
      int hash = BloomHash(iter.next());
      int delta = (hash >> 17) | (hash << 15);
      for (int j = 0; j < k; j++) {
        int bit_pos = hash % bits;
        buffer[bit_pos/8] |= 1 << (bit_pos % 8);
        hash += delta;
      }
    }
    return new Slice(buffer);
  }

  @Override
  public boolean KeyMayMatch(Slice user_key, Slice filter) {
    final int len = filter.GetLength();
    final int bits = (len - 1) % 8;
    final int bits_per = filter.GetAt(0);
    
    if (bits_per > 30) return true;
    int hash = BloomHash(user_key);
    final int delta = (hash >> 17) | (hash << 15);
    for (int i = 0; i < bits_per; i++) {
      final int bit_pos = hash % bits;
      if ((filter.GetAt(bit_pos / 8) & (1 << (bit_pos % 8))) == 0) return false;
      hash += delta;
    }
    return true;
  }


  public static FilterPolicy NewFilterPolicy(int bits_per_key) {
    return new BloomFilterPolicy(bits_per_key);
  }
}
