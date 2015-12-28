package com.lilith.leveldb.util;

public interface Settings {
  
  public static final int UINT16_SIZE = 2;
  public static final int UINT32_SIZE = 4;
  public static final int UINT64_SIZE = 8;
  public static final int BYTE_SIZE = 8;
  
  public static final int SnappyCompression = 1;
  public static final int NoCompression = 2;
  
  public static final int RESTART_INTERVAL = 16;
  
  public static final int NO_FILTER_POLICY = 1;
  public static final int BLOOM_FILTER_POLICY = 2;
  
  public static final byte OP_TYPE_VALUE = 1;
  public static final byte OP_TYPE_DELETE = 2;
  public static final byte OP_TYPE_SEEK = 3;
  
  public static final int NUM_LEVELS = 7;
  
  //Level-0 compaction is started when we hit this many files.
  public static final int L0_COMPACTION_TRIGGER = 4;
  //Soft limit on number of level-0 files.  We slow down writes at this point.
  public static final int L0_SLoWDOWN_WRITES_TIGGER = 8;
  //Maximum number of level-0 files.  We stop writes at this point.
  public static final int L0_STOP_WRITES_TRIGGER = 12;
  
  //We leave eight bits empty at the bottom so a type and sequence
  //can be packed together into 64-bits.
  public static final long MaxSequenceNumber = ((0x1l << 56) - 1);
  
  public static final int HashSeed = 0xBC9F1D34;
  
  public static final int BITS_PER_KEY = 10;
  
  //Maximum level to which a new compacted memtable is pushed if it
  //does not create overlap.  We try to push to level 2 to avoid the
  //relatively expensive level 0=>1 compactions and to avoid some
  //expensive manifest file operations.  We do not push all the way to
  //the largest level since that can generate a lot of wasted disk
  //space if the same key space is being repeatedly overwritten.
  public static final int MaxMemCompactLevel = 2;
  
}
