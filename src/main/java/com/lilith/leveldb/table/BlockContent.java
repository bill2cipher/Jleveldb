package com.lilith.leveldb.table;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.DecodeFailedException;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

/**
 * Represents content of a block.
 * @author Administrator
 *
 */
class BlockContent {
  private Slice data;
  private boolean cachable;
  private boolean heap_allocated;
  
  public BlockContent() { this(Slice.EmptySlice, false, false); };
  
  public BlockContent(Slice data, boolean cachable, boolean heap_allocated) {
    this.SetData(data);
    this.SetCachable(cachable);
    this.SetHeapAllocated(heap_allocated);
  }

  public boolean IsCachable() {
    return cachable;
  }

  public void SetCachable(boolean cachable) {
    this.cachable = cachable;
  }

  public boolean IsHeapAllocated() {
    return heap_allocated;
  }

  public void SetHeapAllocated(boolean heap_allocated) {
    this.heap_allocated = heap_allocated;
  }

  public Slice GetData() {
    return data;
  }

  public void SetData(Slice data) {
    this.data = data;
  }
}


/**
 * Class used to reference a Block in a file.
 * @author Administrator
 *
 */
class BlockHandle {

  private int offset = 0;  // block offset in the stored file
  private int size   = 0;  // block size
  
  // max length of a block could be
  public static final int MaxEncodedLength = 4 + 4;
  
  public static final BlockHandle NullBlockHandle = new BlockHandle(-1, -1);
  
  public BlockHandle() { }
  
  public BlockHandle(int offset, int size) {
    this.offset = offset;
    this.size = size;
  }
  
  public int GetOffset() { 
    return this.offset; 
  }
  
  public void SetOffset(int offset) {
    this.offset = offset;
  }
  
  public int GetSize() {
    return this.size;
  }
  
  public void SetSize(int size) {
    this.size = size;
  }
  
  public void EncodeTo(byte[] buffer, int buffer_offset) {
    BinaryUtil.PutVarint32(buffer, buffer_offset, this.offset);
    BinaryUtil.PutVarint32(buffer, buffer_offset + Settings.UINT32_SIZE, this.size);
  }
  
  public void DecodeFrom(byte[] buffer, int buffer_offset) {
    this.offset = BinaryUtil.DecodeVarint32(buffer, buffer_offset);
    this.size   = BinaryUtil.DecodeVarint32(buffer, buffer_offset + Settings.UINT32_SIZE);
  }
}


/**
 * At the end of every table, there's a Footer encapsulates the fixed information. 
 * @author Administrator
 *
 */
class Footer {
  private BlockHandle metaindex_handle = BlockHandle.NullBlockHandle;
  private BlockHandle index_handle = BlockHandle.NullBlockHandle;
  public final static int EncodedLength = BlockHandle.MaxEncodedLength * 2 + 8;
  
  public void SetMetaIndexHandle(BlockHandle metaindex_handle) {
    this.metaindex_handle = metaindex_handle;
  }
  
  public BlockHandle GetMetaIndexHandle() {
    return this.metaindex_handle;
  }
  
  public void SetIndexHandle(BlockHandle index_handle) {
    this.index_handle = index_handle;
  }
  
  public BlockHandle GetIndexHandle() {
    return this.index_handle;
  }
  
  public void EncodeTo(byte[] buffer, int offset) {
    metaindex_handle.EncodeTo(buffer, offset);
    index_handle.EncodeTo(buffer, offset + Settings.UINT32_SIZE * 2);
    BinaryUtil.PutVarint32(buffer, offset + Settings.UINT32_SIZE * 4, (int)(0XFFFFFFFF & Table.TableMagicNumber));
    BinaryUtil.PutVarint32(buffer, offset + Settings.UINT32_SIZE * 5, (int)(Table.TableMagicNumber >> 32));
  }
  
  public void DecodeFrom(Slice input) throws DecodeFailedException {
    int magic_tail = BinaryUtil.DecodeVarint32(input.GetData(), input.GetOffset() + Settings.UINT32_SIZE * 4);
    int magic_head = BinaryUtil.DecodeVarint32(input.GetData(), input.GetOffset() + Settings.UINT32_SIZE * 5);
    if ((magic_tail != (int)(0XFFFFFFFF & Table.TableMagicNumber)) || (magic_head != (int)(Table.TableMagicNumber >> 32)))
      throw new DecodeFailedException("magic number not match");
    metaindex_handle.DecodeFrom(input.GetData(), input.GetOffset());
    index_handle.DecodeFrom(input.GetData(), input.GetOffset() + Settings.UINT32_SIZE * 2);
  }
}
