package com.lilith.leveldb.version;

public class FileMetaData {
  public long number = 0;
  public int  file_size = 0;
  public InternalKey smallest = null;
  public InternalKey largest = null;
  
  public FileMetaData(long num, int file_size, InternalKey smallest, InternalKey largest) {
    this.number = num;
    this.file_size = file_size;
    this.smallest = smallest;
    this.largest = largest;
  }
}

