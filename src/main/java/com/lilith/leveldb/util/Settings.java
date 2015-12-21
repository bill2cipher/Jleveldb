package com.lilith.leveldb.util;

public interface Settings {
  
  public static int UINT16_SIZE = 2;
  public static int UINT32_SIZE = 4;
  public static int UINT64_SIZE = 8;
  public static int BYTE_SIZE = 8;
  
  public static int SnappyCompression = 1;
  public static int NoCompression = 2;
  
  public static int RESTART_INTERVAL = 16;
  
  public static int NO_FILTER_POLICY = 1;
  public static int BLOOM_FILTER_POLICY = 2;
  
  public static byte OP_TYPE_VALUE = 1;
  public static byte OP_TYPE_DELETE = 2;
  public static byte OP_TYPE_SEEK = 3;
  
  public static int NUM_LEVELS = 7;
  
  //Level-0 compaction is started when we hit this many files.
  public static int L0_COMPACTION_TRIGGER = 4;
  //Soft limit on number of level-0 files.  We slow down writes at this point.
  public static int L0_SLoWDOWN_WRITES_TIGGER = 8;
  //Maximum number of level-0 files.  We stop writes at this point.
  public static int L0_STOP_WRITES_TRIGGER = 12;
  
  //We leave eight bits empty at the bottom so a type and sequence
  //can be packed together into 64-bits.
  public static long MaxSequenceNumber = ((0x1l << 56) - 1);
  
  public static int HashSeed = 0xBC9F1D34;
  
  public static int BITS_PER_KEY = 10;
  
}
