package com.lilith.leveldb.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class FileLocker {
  public FileOutputStream writer = null;
  public FileChannel channel = null;
  public FileLock lock = null;
  
  public FileLocker(FileOutputStream writer, FileChannel channel, FileLock lock) {
    this.writer = writer;
    this.channel = channel;
    this.lock = lock;
  }
  
  public FileLocker() {
    
  }
  
  public void Close() {
    try {
      lock.release();
      channel.close();
      writer.close();
    } catch (IOException exp) {
      Log.error("close lock file error: " + exp.getMessage());
    }
  }
}
