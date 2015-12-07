package com.lilith.leveldb.version;

import com.lilith.leveldb.api.Slice;

public class FileMetaData {
  public long number = 0;
  public int  file_size = 0;
  public Slice smallest = Slice.EmptySlice;
  public Slice largest = Slice.EmptySlice;
}

