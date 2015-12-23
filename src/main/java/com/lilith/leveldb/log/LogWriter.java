package com.lilith.leveldb.log;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class LogWriter {
  private DataOutputStream writer = null;
  private int block_offset = 0;             // current offset in block
  private CRC32 crc32 = null;
  
  public LogWriter(DataOutputStream writer) {
    this.writer = writer;
    this.crc32 = new CRC32();
  }
  
  /**
   * Fragment the record if necessary and emit it. Note that if slice
   * is empty, we still want to iterate once to emit a single zero-length
   * record
   * @throws IOException 
   */
  public void AddRecord(Slice slice) throws IOException {
    int offset = slice.GetOffset();
    int left   = slice.GetLength();
    byte[] data = slice.GetData();
    boolean begin = true, end = false;
    byte record_type = 0;
    if (LogFormat.BLOCK_SIZE - block_offset < LogFormat.HEADER_SIZE) {
      writer.write(LogFormat.ZERO_BYTES, 0, LogFormat.BLOCK_SIZE - block_offset);
      block_offset = 0;
    }
    
    int frag_size = 0;
    while (left > 0) {
      end = left < (LogFormat.BLOCK_SIZE - block_offset - LogFormat.HEADER_SIZE) ? true : false;
      if (begin & end) record_type = LogFormat.FULL;
      else if (begin) record_type = LogFormat.FIRST;
      else if (end)   record_type = LogFormat.LAST;
      else record_type = LogFormat.MIDDLE;
      frag_size = EmitPhysicalRecord(record_type, data, offset, left);
      left -= frag_size;
      offset += frag_size;
      begin = false;
    }
  }
  
  public void Close() throws IOException {
    if (writer != null) writer.close();
  }
  
  
  private int EmitPhysicalRecord(byte record_type, byte[] data, int offset, int size) throws IOException { 
    int block_avail = LogFormat.BLOCK_SIZE - LogFormat.HEADER_SIZE - block_offset;
    int frag_size = block_avail > size ? size : block_avail;
    
    crc32.update(data, offset, frag_size);
    long checksum = crc32.getValue();
    
    byte[] buffer = new byte[LogFormat.HEADER_SIZE];
    BinaryUtil.PutVarint64(buffer, 0, checksum);
    BinaryUtil.PutVarint16(buffer, Settings.UINT64_SIZE, frag_size);
    buffer[Settings.UINT16_SIZE + Settings.UINT64_SIZE] = record_type;
    writer.write(buffer, 0, LogFormat.HEADER_SIZE);
    
    block_offset += LogFormat.HEADER_SIZE + frag_size;
    if (frag_size > 0) 
      writer.write(data, offset, frag_size);
    return frag_size;
  }
}
