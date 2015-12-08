package com.lilith.leveldb.util;

import java.io.File;
import java.util.ArrayList;

public class Util {
  /**
   * Create a directory with the given path.
   */
  public static boolean CreateDir(String path) {
    File dir = new File(path);
    if (dir.exists()) return true;
    try{
      dir.mkdir(); return true;
    } catch (SecurityException error) {
      return false;
    }
  }
  
  /**
   * Test if a given file exists
   */
  public static boolean FileExists(String path) {
    File file = new File(path);
    if (file.exists()) return true;
    return false;
  }
  
  /**
   * Get all the files in the given dir
   */
  public static void GetChildren(String path, ArrayList<String> files) {
    File file = new File(path);
    if (!file.isDirectory()) return;
    File[] children = file.listFiles();
    for (int i = 0; i < children.length; i++) {
      files.add(children[i].getName());
    }
  }
  
  
}
