package com.lilith.leveldb.version;


import java.util.Comparator;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class InternalKeyComparator {
  
  Comparator<Slice> user_comparator = null;

  public static String ComparatorName() {
    return "leveldb.InternalKeyComparator";
  }
  
  public InternalKeyComparator(Comparator<Slice> comp) {
    this.user_comparator = comp;
  }
  
  public int Compare(InternalKey fkey, InternalKey skey) {
    return Compare(fkey.Encode(), skey.Encode());
  }
  
  /**
   * Order by:
   * 1. increasing user key
   * 2. decreasing sequence number
   * 3. decreasing type
   */
  public int Compare(Slice fkey, Slice skey) {
    int res = user_comparator.compare(InternalKey.ExtractUserKey(fkey)
                                    , InternalKey.ExtractUserKey(skey));
    if (res == 0) {
      long fnum = BinaryUtil.DecodeVarint64(fkey.GetData(), fkey.GetOffset() + fkey.GetLength() - Settings.UINT64_SIZE);
      long snum = BinaryUtil.DecodeVarint64(skey.GetData(), skey.GetOffset() + skey.GetLength() - Settings.UINT64_SIZE);
      if (fnum > snum) res = -1;
      else if (fnum < snum) res = 1;
    }
    return res;
  }
  
  public void FindShortestSeparator(String start, Slice limit) {
    
  }
  
  public void FindShortSuccessor(String key) {
    
  }

}
