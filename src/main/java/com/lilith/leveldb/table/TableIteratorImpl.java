package com.lilith.leveldb.table;

import java.io.IOException;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.util.ReadOptions;

public class TableIteratorImpl implements TableIterator {
  
  private ReadOptions options = null;
  private Table table = null;
  private BlockIterator index_iter = null;
  private BlockIterator data_iter = null;
  private Slice data_block_handle = null;
  
  public TableIteratorImpl(ReadOptions options, BlockIterator index_iter, Table table) {
    this.options = options;
    this.index_iter = index_iter;
    this.data_iter = null;
    this.table = table;
  }
  
  public void Seek(Slice target) throws BadFormatException, IOException {
    index_iter.Seek(target);
    InitDataBlock();
    if (data_iter != null) data_iter.Seek(target);
    SkipEmptyDataBlockForward();
  }
  
  public void SeekToLast() throws BadFormatException, IOException {
    index_iter.SeekToLast();
    InitDataBlock();
    if (data_iter != null) data_iter.SeekToLast();
    SkipEmptyDataBlockBackward();
  }
  
  public void SeekToFirst() throws BadFormatException, IOException {
    index_iter.SeekToFirst();
    InitDataBlock();
    if (data_iter != null) data_iter.SeekToFirst();
    SkipEmptyDataBlockForward();
  }
  
  public void Next() throws BadFormatException, IOException {
    data_iter.Next();
    SkipEmptyDataBlockForward();
  }
  
  public void Prev() throws BadFormatException, IOException {
    data_iter.Prev();
    SkipEmptyDataBlockBackward();
  }
  
  public Slice Key() {
    if (!Valid()) return null;
    return data_iter.Key();
  }
  
  public Slice Value() {
    if (!Valid()) return null;
    return data_iter.Value();
  }
  
  public boolean Valid() {
    return (data_iter != null && data_iter.Valid());
  }
  
  private void SkipEmptyDataBlockForward() throws BadFormatException, IOException {
    while (data_iter == null || !data_iter.Valid()) {
      if (!index_iter.Valid()) {
        data_iter = null;
        return;
      }
      
      index_iter.Next();
      InitDataBlock();
      if (data_iter != null) data_iter.SeekToFirst();
    }
  }
  
  private void SkipEmptyDataBlockBackward() throws BadFormatException, IOException {
    while (data_iter == null || !data_iter.Valid()) {
      if (!index_iter.Valid()) {
        data_iter = null;
        return;
      }
      index_iter.Prev();
      InitDataBlock();
      if (data_iter != null) data_iter.SeekToLast();
    }
  }
  
  private void InitDataBlock() throws IOException, BadFormatException {
    if (!index_iter.Valid()) {
      data_iter = null;
      return;
    }
    Slice handle = index_iter.Value();
    if (data_iter != null && handle.compareTo(data_block_handle) == 0) {
      
    } else {
      data_iter = table.BlockReader(options, handle);
      data_block_handle = handle;
    }
  }
}
