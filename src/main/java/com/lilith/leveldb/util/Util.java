package com.lilith.leveldb.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
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
  
  public static FileLocker LockTable(String lockfile) throws FileNotFoundException {
    try {
      FileOutputStream file_stream = new FileOutputStream(lockfile);
      FileChannel channel = file_stream.getChannel();
      FileLock    file_lock = channel.tryLock();
      return new FileLocker(file_stream, channel, file_lock); 
    } catch (FileNotFoundException exp) {
      Log.fatal("fetch or create lock file error: " + exp.getMessage());
      return null;
    } catch (ClosedChannelException exp) {
      Log.error("lock file aquired by another thread: " + exp.getMessage());
      return null;
    } catch (IOException exp) {
      Log.error("lock file failed: " + exp.getMessage());
      return null;
    }
  }
  
  public static int Hash(byte[] data, int offset, int size, int seed) {
    // Similar to murmur hash
    final int m = 0xc6a4a793;
    final int r = 24;
    final int limit = size + offset;
    int h = seed ^ (size * m);

    // Pick up four bytes at a time
    while (offset + Settings.UINT32_SIZE <= limit) {
      int w = BinaryUtil.DecodeVarint32(data, offset);
      offset += Settings.UINT32_SIZE;
      h += w;
      h *= m;
      h ^= (h >> 16);
    }

    // Pick up remaining bytes
    switch (limit - offset) {
      case 3:
        h += data[2] << 16;
      case 2:
        h += data[1] << 8;
      case 1:
        h += data[0];
        h *= m;
        h ^= (h >> r);
        break;
    }
    return h;
  }
}
