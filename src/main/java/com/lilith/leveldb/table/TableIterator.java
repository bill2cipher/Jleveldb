package com.lilith.leveldb.table;

import java.io.IOException;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.exceptions.DecodeFailedException;

public interface TableIterator {
  
  public void Seek(Slice target) throws BadFormatException, IOException, DecodeFailedException;
  
  public void SeekToLast() throws BadFormatException, IOException, DecodeFailedException;
  
  public void SeekToFirst() throws BadFormatException, IOException, DecodeFailedException;
  
  public void Next() throws BadFormatException, IOException, DecodeFailedException;
  
  public void Prev() throws BadFormatException, IOException, DecodeFailedException;
  
  public Slice Key();
  
  public Slice Value();
  
  public boolean Valid();
}
