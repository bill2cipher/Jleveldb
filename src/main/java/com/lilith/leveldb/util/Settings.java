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
  
  public static int NUM_LEVELS = 7;
  
  //Level-0 compaction is started when we hit this many files.
  public static int L0_COMPACTION_TRIGGER = 4;
  //Soft limit on number of level-0 files.  We slow down writes at this point.
  public static int L0_SLoWDOWN_WRITES_TIGGER = 8;
  //Maximum number of level-0 files.  We stop writes at this point.
  public static int L0_STOP_WRITES_TRIGGER = 12;
  
}
