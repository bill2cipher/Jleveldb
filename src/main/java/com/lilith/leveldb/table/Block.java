package com.lilith.leveldb.table;

import java.io.DataInputStream;
import java.io.IOException;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;


public class Block {
  
  public final static int BlockTrailerSize  = 5;
  public final static int NoCompression     = 1;
  public final static int SnappyCompression = 2;
  
  private byte[] data;                 // data stored in this block
  private int size = 0;                // size of this block
  private int restart_offset = 0;      // where the restart point entry is
  private boolean owned;               // is the mem owned by leveldb

  
  public Block(final BlockContent content) {
    this.data = content.GetData().GetData();
    this.size = content.GetData().GetLength();
    this.owned = content.IsHeapAllocated();
    
    int max_restarts_allowed = (size - Settings.UINT32_SIZE) / Settings.UINT32_SIZE;
    if (NumRestarts() > max_restarts_allowed) size = 0;
    else this.restart_offset = size - (1 + NumRestarts()) * Settings.UINT32_SIZE;
  }

  /**
   * Build a new iterator for this block
   * @return
   */
  public BlockIterator Iterator() {
    return new BlockIterator(data, NumRestarts());
  }
  
  private int NumRestarts() {
    return BinaryUtil.DecodeVarint32(data, size - Settings.UINT32_SIZE);
  }
  
  public static Slice DecodeEntry(byte[] data, int offset, int[] values, int limit) {
    if (limit - offset < Settings.UINT32_SIZE * 3) return null;
    int shared = BinaryUtil.DecodeVarint32(data, offset);
    int not_shared = BinaryUtil.DecodeVarint32(data, Settings.UINT32_SIZE + offset);
    int value_size = BinaryUtil.DecodeVarint32(data, Settings.UINT32_SIZE * 2 + offset);
    values[0] = shared;
    values[1] = not_shared;
    values[3] = value_size;
    if ((limit - offset) < not_shared + value_size) return null;
    return new Slice(data, offset + Settings.UINT32_SIZE, not_shared + value_size);
  }
  
  public static boolean ReadBlock(DataInputStream reader, BlockHandle handle, BlockContent content) throws IOException {
    content.SetCachable(true);
    content.SetHeapAllocated(true);
    content.SetData(new Slice(new byte[0]));
    
    int block_size = handle.GetSize();
    byte[] buffer = new byte[block_size + Block.BlockTrailerSize];
    int read_cnt = reader.read(buffer, handle.GetOffset(), block_size  + Block.BlockTrailerSize);
    if (read_cnt != block_size + Block.BlockTrailerSize) return false;
    switch((int)buffer[block_size]) {
    case Block.NoCompression:
      content.SetData(new Slice(buffer, block_size));
      break;
    case Block.SnappyCompression:
      content.SetData(new Slice(buffer, block_size));
    default:
      return false;        
    }
    return true;
  }
}
