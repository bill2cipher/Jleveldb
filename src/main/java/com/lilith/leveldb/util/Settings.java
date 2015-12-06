package com.lilith.leveldb.util;

public interface Settings {
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
}
