package com.lilith.leveldb.impl;

public class CompactionStats {
  public long micros;
  public long bytes_read;
  public long bytes_written = 0;
  
  public void Add(CompactionStats c) {
    this.micros += c.micros;
    this.bytes_read += c.bytes_read;
    this.bytes_written += c.bytes_written;
  }
}