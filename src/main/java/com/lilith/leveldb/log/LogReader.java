package com.lilith.leveldb.log;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.zip.CRC32;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class LogReader {
  private DataInputStream reader = null;   // reader provide input stream.
  private boolean checksum = false;
  private byte[]  buffer = null;
  private boolean eof = false;             // last Read() indicated EOF by returning < BlockSize
  
  private int last_record_offset = 0;     // offset of the last record returned by ReadRecord.
  private int start_of_buffer_offset = 0; // where to start the parse of next record in buffer
  private int end_of_buffer_offset = 0;   // offset of the first location past the end of buffer.
  private int initial_offset = 0;         // offset at which to start looking for the first record to return.
  
  private final byte  EOF = LogFormat.MAX_RECORD_TYPE + 1;
  private final byte  BAD_RECORD = LogFormat.MAX_RECORD_TYPE + 2;
  
  private CRC32 crc32 = null;

  /**
   * Create a reader that will return log records from the given input stream.
   * The given input stream must stay alive while this reader is in use.
   * 
   * If "checksum" is true, verify checksums if available.
   * 
   * The Reader will start reading at the first record located at physical
   * position >= init_offset within the file.
   */
  public LogReader(DataInputStream reader, boolean checksum, int initial_offset) {
    this.reader = reader;
    this.checksum = checksum;
    this.initial_offset = initial_offset;
    this.buffer = new byte[LogFormat.BLOCK_SIZE];
    this.eof = false;
    this.last_record_offset = 0;
    this.start_of_buffer_offset = 0;
    this.end_of_buffer_offset = 0;
    this.crc32 = new CRC32();
  }
  
  public byte[] ReadRecord() throws IOException {
    if (last_record_offset < initial_offset) {
      if (!SkipToInitialBlock()) return null;
    }
    
    int prospective_record_offset = 0;        // Record offset of the logical record that we're reading
    Slice fragment = new Slice(new byte[0]);  // store each parsed record from block
    LinkedList<byte[]> store = new LinkedList<byte[]>();   // store all data
    byte[] data = null;    // store every piece of record at each cycle
    
    while (true) {
      byte record_type = ReadPhysicalRecord(fragment);
      switch (record_type) {
      case LogFormat.FULL:
        last_record_offset = start_of_buffer_offset;
        data = new byte[fragment.GetLength()];
        BinaryUtil.CopyBytes(fragment.GetData(), fragment.GetOffset(), fragment.GetLength(), data, 0);
        return data;
      case LogFormat.FIRST:
        prospective_record_offset = start_of_buffer_offset;
        data = new byte[fragment.GetLength()];
        BinaryUtil.CopyBytes(fragment.GetData(), fragment.GetOffset(), fragment.GetLength(), data, 0);
        store.add(data);
        break;
      case LogFormat.LAST:
        data = new byte[fragment.GetLength()];
        BinaryUtil.CopyBytes(fragment.GetData(), fragment.GetOffset(), fragment.GetLength(), data, 0);
        last_record_offset = prospective_record_offset;
        return ConvertByteListToBytes(store);
      case LogFormat.MIDDLE:
        data = new byte[fragment.GetLength()];
        BinaryUtil.CopyBytes(fragment.GetData(), fragment.GetOffset(), fragment.GetLength(), data, 0);
        store.add(data);
        break;
      case BAD_RECORD:
        return null;
      case EOF:
        return null;
      }
    }
  }

  
  
  /**
   * Returns the physical offset of the last record returned by ReadRecord.
   * 
   * Undefined before the first call to ReadRecord.
   * @return
   */
  public int LastRecordOffset() {
    return this.last_record_offset;
  }
  
  private byte[] ConvertByteListToBytes(LinkedList<byte[]> store) {
    int size = 0;
    Iterator<byte[]> iter = store.iterator();
    while (iter.hasNext()) {
      size += iter.next().length;
    }
    
    
    byte[] result = new byte[size];
    Iterator<byte[]> val_iter = store.iterator();
    int offset = 0;
    while (val_iter.hasNext()) {
      byte[] data = val_iter.next();
      BinaryUtil.CopyBytes(data, 0, data.length, result, offset);
      offset += data.length;
    }
    return result;
  }
  
  private boolean SkipToInitialBlock() {
    int offset_in_block = initial_offset % LogFormat.BLOCK_SIZE;
    int block_start_location = initial_offset - offset_in_block;
    if (offset_in_block > LogFormat.BLOCK_SIZE - 6) {
      offset_in_block = 0;
      block_start_location += LogFormat.BLOCK_SIZE;
    }
    
    end_of_buffer_offset = block_start_location;
    
    if (block_start_location > 0) {
      try {
        reader.skip(block_start_location);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return false;
      }
    }
    return true;
  }
  
  private byte ReadPhysicalRecord(Slice result) throws IOException {
    while (true) {
      // read from file if there's not enough data to be parsed.
      if ((end_of_buffer_offset - start_of_buffer_offset) < LogFormat.HEADER_SIZE) {
        if (!eof) {
          start_of_buffer_offset = 0;
          end_of_buffer_offset = reader.read(buffer, 0, LogFormat.BLOCK_SIZE);
          if (end_of_buffer_offset < LogFormat.BLOCK_SIZE) eof = true;
          continue;
        } else {
          start_of_buffer_offset = 0;
          end_of_buffer_offset = 0;
          return EOF;
        }
      }
      
      int crc32_val = BinaryUtil.DecodeVarint32(buffer, 0);
      int length = BinaryUtil.DecodeVarint16(buffer, Settings.UINT32_SIZE);
      byte record_type = buffer[Settings.UINT16_SIZE + Settings.UINT32_SIZE];
      
      if (LogFormat.HEADER_SIZE + length + start_of_buffer_offset > end_of_buffer_offset) {
        end_of_buffer_offset = 0;
        start_of_buffer_offset = 0;
        if (!eof) return BAD_RECORD;
        return EOF;
      }
      
      if (record_type == LogFormat.ZERO && length == 0) {
        end_of_buffer_offset = 0;
        start_of_buffer_offset = 0;
        return BAD_RECORD;
      }
      
      if (checksum) {
        crc32.update(buffer, start_of_buffer_offset + LogFormat.HEADER_SIZE, length);
        int expected_crc32 = (int) crc32.getValue();
        if (crc32_val != expected_crc32) {
          end_of_buffer_offset = 0;
          start_of_buffer_offset = 0;
          return BAD_RECORD;
        }
      }

      result.SetContent(buffer, start_of_buffer_offset + LogFormat.HEADER_SIZE, length);
      start_of_buffer_offset += LogFormat.HEADER_SIZE + length;
      return record_type;
    }
  }
}
