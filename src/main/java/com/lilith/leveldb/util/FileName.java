package com.lilith.leveldb.util;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileName {
  
  public static final int LOG_FILE = 1;
  public static final int TABLE_FILE = 2;
  public static final int SSTTABLE_FILE = 3;
  public static final int DESC_FILE = 4;
  public static final int CURRENT_FILE = 5;
  public static final int LOCK_FILE = 6;
  public static final int TMP_FILE = 7;
  public static final int INFOLOG_FILE = 9;
  public static final int INVALID_FILE = 10;
  
  // Combines the given data to make a file name.
  private static String MakeFileName(String name, long number, String suffix) {
    return String.format("%s/%06d.%s", name, number, suffix);
  }
  
  public static String LogFileName(String name, long number) {
    return MakeFileName(name, number, "log");
  }
  
  public static String TableFileName(String name, long number) {
    return MakeFileName(name, number, "ldb");
  }
  
  public static String SSTTableFileName(String name, long number) {
    return MakeFileName(name, number, "sst");
  }
  
  public static String DescriptorFileName(String name, long number) {
    return String.format("%s/MANIFEST-%06d", name, number);
  }
  
  public static String CurrentFileName(String name) {
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
    File file = new File(filename);
    String short_name = file.getName();
    String extension = "";
    int dot_index = filename.lastIndexOf('.');
    if ((dot_index > -1) && (dot_index < filename.length() - 1)) {
      extension = filename.substring(dot_index + 1); 
    }
    
    if (short_name.equals("CURRENT")) {
      return CURRENT_FILE;
    } else if (short_name.equals("LOCK")) {
      return LOCK_FILE;
    } else if (short_name.equals("LOG") || short_name.equals("LOG.old")) {
      return INFOLOG_FILE;
    } else if (short_name.startsWith("MANIFEST")) {
      return DESC_FILE;
    } else if (extension.equals("log")) {
      return LOG_FILE;
    } else if (extension.equals("sst") || extension.equals("ldb")) {
      return TABLE_FILE;
    } else if (extension.equals("dbtmp")) {
      return TMP_FILE;
    } else return INVALID_FILE;
  }
  
  public static long  ParseFileNumber(String filename, int file_type) {
    File file = new File(filename);
    String short_name = file.getName(), number = "";
    int dot_index = filename.lastIndexOf('.');
    
    switch(file_type) {
    case CURRENT_FILE:
    case LOCK_FILE:
    case INFOLOG_FILE:
    case TMP_FILE:
      return -1;
    case DESC_FILE:
      number = short_name.substring("MANIFEST-".length());
      return Long.valueOf(number);
    case LOG_FILE:
    case TABLE_FILE:
      number = short_name.substring(0, dot_index);
      return Long.valueOf(number);
    default:
        return -1;
    }
  }
  
  public static void SetCurrentFile(String dbname, long num) throws IOException {
    DataOutputStream writer = new DataOutputStream(new FileOutputStream(CurrentFileName(dbname)));
    String manifest = DescriptorFileName(dbname, num);
    if (manifest.startsWith(dbname + "/")) {
      manifest.substring((dbname + "/").length(), manifest.length());
    }
    writer.write((manifest + "\n").getBytes());
    writer.flush();
    writer.close();
  }
}
