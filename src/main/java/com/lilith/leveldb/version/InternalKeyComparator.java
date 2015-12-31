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
  
  public Comparator GetUserComparator() {
    return user_comparator;
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
    Slice user_key_lm = InternalKey.ExtractUserKey(limit);
    Slice sep = user_comparator.FindShortestSeparator(user_key_st, user_key_lm);
    if (sep.GetLength() < user_key_st.GetLength() && user_comparator.Compare(user_key_st, sep) < 0) {
      byte[] buffer = new byte[sep.GetLength() + Settings.UINT64_SIZE];
      BinaryUtil.CopyBytes(sep.GetData(), sep.GetOffset(), sep.GetLength(), buffer, 0);
      BinaryUtil.PutVarint64(buffer, sep.GetLength(), (Settings.MaxSequenceNumber << 8) | (Settings.OP_TYPE_SEEK & 0XFF));
      return new Slice(buffer);
    }
    return null;
  }

  @Override
  public Slice FindShortestSuccessor(Slice key) {
    Slice user_key = InternalKey.ExtractUserKey(key);
    Slice suc = user_comparator.FindShortestSuccessor(user_key);
    if (suc.GetLength() < user_key.GetLength() && user_comparator.Compare(suc, user_key) < 0) {
      byte[] buffer = new byte[suc.GetLength() + Settings.UINT64_SIZE];
      BinaryUtil.CopyBytes(user_key.GetData(), user_key.GetOffset(), user_key.GetLength(), buffer, 0);
      BinaryUtil.PutVarint64(buffer, user_key.GetLength(), (Settings.MaxSequenceNumber << 8) | (Settings.OP_TYPE_SEEK & 0XFF));
      return new Slice(buffer);
    }
    return null;
  }

}
