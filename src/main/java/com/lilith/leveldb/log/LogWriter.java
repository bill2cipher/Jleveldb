package com.lilith.leveldb.log;

import java.io.DataOutputStream;
import java.util.zip.CRC32;

import com.lilith.leveldb.api.Slice;

public class LogWriter {
  private DataOutputStream writer = null;
  private int[] type_crc;
  private int block_offset = 0;             // current offset in block
  
  public LogWriter(DataOutputStream writer) {
    this.writer = writer;
    this.type_crc = new int [LogFormat.MAX_RECORD_TYPE + 1];
    
    CRC32 crc32 = new CRC32();
    for (int i = 0; i <= LogFormat.MAX_RECORD_TYPE; i++) {
      crc32.update(i);
      type_crc[i] = (int) crc32.getValue();
    }
  }
  
  /**
   * Fragment the record if necessary and emit it. Note that if slice
   * is empty, we still want to iterate once to emit a single zero-length
   * record
   */
  public boolean AddRecord(Slice slice) {
    int offset = slice.GetOffset();
    int left   = slice.GetLength();
    byte[] data = slice.GetData();
    boolean begin = true;
    int leftover = LogFormat.BLOCK_SIZE - block_offset;
    while (left > 0) {
      leftover = LogFormat.BLOCK_SIZE - block_offset;
      if (leftover <= 6) FillBlockWithZero();
      else if (leftover == 7) FillBlockWithZeroFirst();
      else if (begin == true) FillBlockWithFirst();
      else if (left <= (LogFormat.BLOCK_SIZE - LogFormat.HEADER_SIZE)) FillBlockWithLast();
      else FillBlockWithMiddle();
    }
  }
  
  private boolean EmitPhysicalRecord(int record_type, Slice slice) {
    return false;
  }
}
