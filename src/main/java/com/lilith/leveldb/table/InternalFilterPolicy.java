package com.lilith.leveldb.table;

import java.util.ArrayList;
import java.util.Iterator;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.version.InternalKey;

/**
 * Filter policy wrapper that converts from internal keys to user keys 
 */
public class InternalFilterPolicy extends FilterPolicy {
  private FilterPolicy user_policy = null;
  
  public InternalFilterPolicy(FilterPolicy policy) {
    this.user_policy = policy;
  }

  @Override
  public Slice Name() {
    return user_policy.Name();
  }

  @Override
  public Slice CreateFilter(ArrayList<Slice> keys) {
    ArrayList<Slice> user_keys = new ArrayList<Slice>(keys.size());
    Iterator<Slice> iter = keys.iterator();
    while (iter.hasNext()) {
      user_keys.add(InternalKey.ExtractUserKey(iter.next()));
    }
    return user_policy.CreateFilter(user_keys);
  }
  
  @Override
  public boolean KeyMayMatch(Slice key, Slice filter) {
    return user_policy.KeyMayMatch(InternalKey.ExtractUserKey(key), filter);
  }

}
