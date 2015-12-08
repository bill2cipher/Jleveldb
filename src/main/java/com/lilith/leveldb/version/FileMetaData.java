package com.lilith.leveldb.version;

import com.lilith.leveldb.api.Slice;

public class FileMetaData {
  public long number = 0;
  public int  file_size = 0;
  public Slice smallest = Slice.EmptySlice;
  public Slice largest = Slice.EmptySlice;
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj.getClass() == FileMetaData.class) {
      FileMetaData file = (FileMetaData) obj;
      return (smallest.compareTo(file.smallest) == 0) && number == file.number;    
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return (int) number;
  }
}

