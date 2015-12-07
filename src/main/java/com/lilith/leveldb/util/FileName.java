package com.lilith.leveldb.util;

public class FileName {
  
  public static final int LOG_FILE = 1;
  public static final int TABLE_FILE = 2;
  public static final int SSTTABLE_FILE = 3;
  public static final int DESC_FILE = 4;
  public static final int CURRENT_FILE = 5;
  public static final int LOCK_FILE = 6;
  public static final int TMP_FILE = 7;
  public static final int INFOLOG_FILE = 8;
  
  // Combines the given data to make a file name.
  private static String MakeFileName(String name, long number, String suffix) {
    return String.format("%s/%06d.%s", name, number, suffix);
  }
  
  public static String MakeLogName(String name, long number) {
    return MakeFileName(name, number, "log");
  }
  
  public static String TableFileName(String name, long number) {
    return MakeFileName(name, number, "ldb");
  }
  
  public static String SSTTableFileName(String name, long number) {
    return MakeFileName(name, number, "sst");
  }
  
  public static String DescriptorFileName(String name, long number) {
    return MakeFileName(name, number, "MANIFEST");
  }
  
  public static String CurrentFileName(String name, long number) {
    return name + "/CURRENT";
  }
  
  public static String LockFileName(String dbname) {
    return dbname + "/LOCK";
  }
  
  public static String TempFileName(String dbname, long number) {
    return MakeFileName(dbname, number, "dbtmp");
  }
  
  public static String InfoLogFileName(String dbname) {
    return dbname + "/LOG";
  }
  
  public static String OldInfoLogFileName(String dbname) {
    return dbname + "/LOG.old";
  }
  
  public static int ParseFileType(String filename) {
    return 0; 
  }
  
  public static int ParseFileNumber(String filename) {
    return 0;
  }
}
