package com.lilith.leveldb.util;

public class WriteOptions {
  // If true, the write will be flushed from the operating system buffer cache before
  // the write is considered complete. If this flag is true, writes will be slower.
  // 
  // If this flag is false, and the machine crashes, some recent writes may be lost.
  // Not that if it is just the process that crashes, no writes will be lost even if
  // sync equals false.
  //
  // In other words, a DB write with sync == false has similar crash semantics as the
  // "write()" system call. A DB write with sync == true has similar crash semantics to
  // a "write()" system call followed by "fsync()"
  public boolean sync = false;
}
