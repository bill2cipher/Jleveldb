package com.lilith.leveldb.table;

import java.io.IOException;
import java.util.ArrayList;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.exceptions.DecodeFailedException;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.version.FileMetaData;
import com.lilith.leveldb.version.InternalKeyComparator;
import com.lilith.leveldb.version.VersionUtil;

public class TableLevelIterator implements TableIterator {
  
  private final InternalKeyComparator icmp;
  private final ArrayList<FileMetaData> files;
  private final TableCache cache;
  private final ReadOptions options;
  private int cur_index = 0;
  private long file_number = 0;
  private TableIterator table_iter = null;
  
  public TableLevelIterator(InternalKeyComparator icmp, ArrayList<FileMetaData> files, TableCache cache, ReadOptions options) {
    this.icmp = icmp;
    this.files = files;
    this.cache = cache;
    this.options = options;
    

    this.cur_index = files.size();
    this.table_iter = null;
    this.file_number = 0;
  }

  public void Seek(Slice target) throws BadFormatException, IOException, DecodeFailedException {
    cur_index = VersionUtil.FindFile(icmp, files, target);
    InitTableIter();
    if (table_iter != null) table_iter.Seek(target);
    SkipEmptyTableForward();
  }

  public void SeekToLast() throws BadFormatException, IOException, DecodeFailedException {
    cur_index = files.size() - 1;
    InitTableIter();
    if (table_iter != null) table_iter.SeekToLast();
    SkipEmptyTableBackward();
  }

  public void SeekToFirst() throws BadFormatException, IOException, DecodeFailedException {
    cur_index = 0;
    InitTableIter();
    if (table_iter != null) table_iter.SeekToFirst();
    SkipEmptyTableForward();
  }

  public void Next() throws BadFormatException, IOException, DecodeFailedException {
    table_iter.Next();
    SkipEmptyTableForward();
  }

  public void Prev() throws BadFormatException, IOException, DecodeFailedException {
    table_iter.Prev();
    SkipEmptyTableBackward();
  }

  public Slice Key() {
    if (!Valid()) return null;
    return table_iter.Key();
  }

  public Slice Value() {
    if (!Valid()) return null;
    return table_iter.Value();
  }

  public boolean Valid() {
    return table_iter != null && table_iter.Valid();
  }
  
  private void SkipEmptyTableForward() throws IOException, DecodeFailedException, BadFormatException {
    while (table_iter == null || !table_iter.Valid()) {
      if (cur_index < 0 || cur_index >= files.size()) {
        table_iter = null;
        return;
      }
      cur_index++;
      InitTableIter();
      if (table_iter != null) table_iter.SeekToFirst();
    }
  }
  
  private void SkipEmptyTableBackward() throws IOException, DecodeFailedException, BadFormatException {
    while (table_iter == null || !table_iter.Valid()) {
      if (cur_index < 0 || cur_index >= files.size()) {
        table_iter = null;
        return;
      }
      cur_index--;
      InitTableIter();
      if (table_iter != null) table_iter.SeekToLast();
    }
  }
  
  private void InitTableIter() throws IOException, DecodeFailedException, BadFormatException {
    if (cur_index < 0 || cur_index >= files.size()) {
      table_iter = null;
      return;
    }
    
    if (file_number == files.get(cur_index).number) {
    } else {
      Table table = cache.FindTable(files.get(cur_index).number, files.get(cur_index).file_size);
      table_iter = table.TableIterator(options);
      file_number = files.get(cur_index).number;
    }
  }
  
}
