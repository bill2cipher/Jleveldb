package com.lilith.leveldb.version;


import com.lilith.leveldb.api.Comparator;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class InternalKeyComparator extends Comparator {
  
  Comparator user_comparator = null;
  
  public InternalKeyComparator(Comparator comp) {
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
  @Override
  public int Compare(Slice fval, Slice sval) {
    int res = user_comparator.Compare(InternalKey.ExtractUserKey(fval)
                                    , InternalKey.ExtractUserKey(sval));
    if (res == 0) {
      long fnum = BinaryUtil.DecodeVarint64(fval.GetData(), fval.GetOffset() + fval.GetLength() - Settings.UINT64_SIZE);
      long snum = BinaryUtil.DecodeVarint64(sval.GetData(), sval.GetOffset() + sval.GetLength() - Settings.UINT64_SIZE);
      if (fnum > snum) res = -1;
    else if (fnum < snum) res = 1;
    }
    return res;
  }

  @Override
  public String Name() {
    return "leveldb.InternalKeyComparator";
  }

  @Override
  public Slice FindShortestSeparator(Slice start, Slice limit) {
    Slice user_key_st = InternalKey.ExtractUserKey(start);
    Slice 
  }

  @Override
  public Slice FindShortestSuccessor(Slice key) {
    // TODO Auto-generated method stub
    return null;
  }

}
