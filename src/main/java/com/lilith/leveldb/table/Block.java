package com.lilith.leveldb.table;

import java.io.DataInputStream;
import java.io.IOException;

import com.lilith.leveldb.api.Comparator;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.Settings;


public class Block {
  
  public final static int BlockTrailerSize  = 5;  // 1-byte type + 32-bit crc
  public final static int NoCompression     = 1;
  public final static int SnappyCompression = 2;
  
  private byte[] data;                 // data stored in this block
  private int size = 0;                // size of this block
  private int offset = 0;              // offset of block in the data
  private int restart_offset = 0;      // where the restart point entry is

  
  public Block(BlockContent content) throws BadFormatException {
    this.data = content.GetData().GetData();
    this.size = content.GetData().GetLength();
    this.offset = content.GetData().GetOffset();
    
    int max_restarts_allowed = (size - Settings.UINT32_SIZE) / Settings.UINT32_SIZE;
    if (size < Settings.UINT32_SIZE || NumRestarts() > max_restarts_allowed)
      throw new BadFormatException("too many restarts within block");
    this.restart_offset = offset + size - (1 + NumRestarts()) * Settings.UINT32_SIZE;
  }
  
  public int Size() {
    return size;
  }

  /**
   * Build a new iterator for this block
   * @return
   */
  public BlockIterator Iterator(Comparator cmp) {
    return new BlockIterator(cmp, data, offset, size, restart_offset, NumRestarts());
  }
  
  private int NumRestarts() {
    return BinaryUtil.DecodeVarint32(data, offset + size - Settings.UINT32_SIZE);
  }
  
  /**
   * decode the next block entry starting at p, storing the number of shared key bytes,
   * non_shared key bytes and the length of the value in values.
   * @throws BadFormatException 
   */
  public static int DecodeEntry(byte[] data, int offset, int[] values, int limit) throws BadFormatException {
    if (limit - offset < 3 * Settings.UINT32_SIZE)
      throw new BadFormatException("block size too small");
    int shared = BinaryUtil.DecodeVarint32(data, offset);
    int not_shared = BinaryUtil.DecodeVarint32(data, Settings.UINT32_SIZE + offset);
    int value_size = BinaryUtil.DecodeVarint32(data, Settings.UINT32_SIZE * 2 + offset);
    values[0] = shared;
    values[1] = not_shared;
    values[3] = value_size;

    if ((limit - offset - Settings.UINT32_SIZE * 3) < (not_shared + value_size))
      throw new BadFormatException("");
    return offset + Settings.UINT32_SIZE * 3;
  }
  
  /**
   * Read the specified block from reader stream and fill the content in content.
   * @throws IOException
   * @throws BadFormatException 
   */
  public static boolean ReadBlock(DataInputStream reader, ReadOptions options, BlockHandle handle, BlockContent content) throws IOException, BadFormatException {
    content.SetCachable(true);
    content.SetHeapAllocated(true);
    content.SetData(Slice.EmptySlice);
    
    int block_size = handle.GetSize();
    byte[] buffer = new byte[block_size + Block.BlockTrailerSize];
    int read_cnt = reader.read(buffer, handle.GetOffset(), block_size  + Block.BlockTrailerSize);
    if (read_cnt != block_size + Block.BlockTrailerSize) return false;
    switch((int)buffer[block_size]) {
    case Block.NoCompression:
      content.SetData(new Slice(buffer, block_size));
      break;
    case Block.SnappyCompression:
      throw new RuntimeException("snappy compresion not supported now");
    default:
      throw new BadFormatException("could not resolve unkown type");      
    }
    return true;
  }
}
