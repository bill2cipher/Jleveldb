package com.lilith.leveldb.impl;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.memtable.MemTable;
import com.lilith.leveldb.memtable.MemTableInserter;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
/**
 * WriteBatch's byte rep formats as following:
 *     sequence: fix64
 *     count   : fix32
 *     data    : record[count]
 * record := 
 *     TypeValue varstring varstring
 *     TypeDeletion varstring 
 * varstring :=
 *     len  : varint32
 *     data : byte [len]
 */
public class WriteBatch {
  
  // internal represents of write batch
  private LinkedList<Slice> batch;
  
  // sequence number of this batch
  private long sequence = 0;
  
  // bytes contain all contents
  private byte[] rep = new byte[0];
  
  // if updated since last rep is generated 
  private boolean updated = true;
  
  /**
   * Constructor for WriteBatch
   * @param sequence
   */
  public WriteBatch(long sequence) {
    batch = new LinkedList<Slice>();
    this.sequence = sequence;
  }
  
  /**
   * Store the key/value pair into the database.
   * @param key
   * @param value
   */
  public void Put(Slice key, Slice value) {
    int offset = 0;
    byte[] buffer = new byte[1 + Settings.UINT32_SIZE + key.GetLength() + Settings.UINT32_SIZE + value.GetLength()];
    buffer[0] = (byte)(0XFF & Settings.OP_TYPE_VALUE);
    
    offset +=1;
    offset = Slice.EncodeLengthPrefix(buffer, offset, key);
    offset = Slice.EncodeLengthPrefix(buffer, offset, value);
    
    batch.add(new Slice(buffer));
    updated = true;
  }
  
  /**
   * If the database contains a mapping key for "key", erase it. Else do nothing.
   * @param key
   */
  public void Delete(Slice key) {
    int offset = 0;
    byte[] buffer = new byte[1 + Settings.UINT32_SIZE + key.GetLength()];
    buffer[0] = (byte)(0XFF & Settings.OP_TYPE_DELETE);
    
    offset +=1;
    offset = Slice.EncodeLengthPrefix(buffer, offset, key);
    
    batch.add(new Slice(buffer));
    updated = false;
  }
  
  /**
   *  Clear all updates buffered in this batch. 
   */
  public void Clear() {
    sequence = 0;
    batch.clear();
    updated = true;
    rep = new byte[0];
  }
  
  /**
   * Return the number of entries in the batch. 
   */
  public int Count() {
    return batch.size();
  }
  
  /**
   * Return the sequence number for the start of this batch. 
   */
  public long Sequence() {
    return this.sequence;
  }
  
  /**
   * Return the byte represents of the write batch.
   * @return
   */
  public Slice Contents() {
    if (updated == false) return new Slice(rep);
    
    int size = ByteSize();
    byte[] content = new byte[size];
    BinaryUtil.PutVarint64(content, 0, this.sequence);
    BinaryUtil.PutVarint32(content, Settings.UINT64_SIZE, batch.size());
    
    Iterator<Slice> iter = batch.iterator();
    int offset = Settings.UINT32_SIZE + Settings.UINT64_SIZE;
    while (iter.hasNext()) {
      Slice v = iter.next();
      BinaryUtil.CopyBytes(v.GetData(), v.GetOffset(), v.GetLength(), content, offset);
      offset += v.GetLength();
    }
    rep = content;
    updated = false;
    return new Slice(content);
  }
  
  /**
   * Decode the write batch from bytes
   * @param content
   */
  public void SetContents(Slice content) {
    this.rep = new byte[content.GetLength()];
    BinaryUtil.CopyBytes(content.GetData(), content.GetOffset(), content.GetLength(), rep, 0);
    
    int offset = 0;
    batch.clear();
    
    this.sequence = BinaryUtil.DecodeVarint64(rep, offset);
    offset += Settings.UINT64_SIZE;
    
    int size = BinaryUtil.DecodeVarint32(rep, offset);
    offset += Settings.UINT32_SIZE;
    
    for (int i = 0; i < size; i++) {
      int op_type = ((int)rep[offset]) & 0XFF;
      int op_len = 0, value_offset;
      Slice key, value;

      switch (op_type) {
      case Settings.OP_TYPE_VALUE:
        key = Slice.DecodeLengthPrefix(rep, offset + 1);
        value_offset = offset + 1 + Settings.UINT32_SIZE + key.GetLength();
        
        value = Slice.DecodeLengthPrefix(rep, value_offset);
        op_len = 1 + Settings.UINT32_SIZE * 2 + value.GetLength() + key.GetLength();
        break;
      case Settings.OP_TYPE_DELETE:
        key = Slice.DecodeLengthPrefix(rep, offset + 1);
        op_len = 1 + Settings.UINT32_SIZE + key.GetLength();
      }
      
      Slice op = new Slice(rep, offset, op_len);
      offset += op_len;
      batch.add(op);
    }
  }
  
  /**
   * Return the size in bytes.
   */
  public int ByteSize() {
    if (updated == false) return rep.length;
    int size = Settings.UINT32_SIZE + Settings.UINT64_SIZE;
    Iterator<Slice> iter = batch.iterator();
    while (iter.hasNext()) {
      Slice v = iter.next();
      size += v.GetLength();
    }
    return size;
  }
  
  /**
   * Return the list contains all operations.
   */
  public List<Slice> OperateList() {
    return batch;
  }
  
  /**
   * Execute the write batch on the specified memtable. 
   * @throws BadFormatException 
   */
  public void InsertInto(MemTable memtable) throws BadFormatException {
    MemTableInserter inserter = new MemTableInserter(sequence, memtable);
    Iterate(inserter);
  }
  
  /**
   * Append another batch at the end of this write batch.
   */
  public void Append(WriteBatch write_batch) {
    Iterator<Slice> iter = write_batch.OperateList().iterator();
    while (iter.hasNext()) {
      batch.add(iter.next());
    }
  }
  
  /**
   * Execute all the operations in the write batch.
   * @throws BadFormatException
   */
  public void Iterate(WriteBatchExecutor executor) throws BadFormatException {
    Iterator<Slice> iter = batch.iterator();
    while (iter.hasNext()) {
      Slice op = iter.next();
      int op_type = ((int)op.GetAt(0)) & 0XFF;
      Slice key = Slice.DecodeLengthPrefix(op.GetData(), op.GetOffset() + 1);
      switch(op_type) {
      case Settings.OP_TYPE_DELETE:
        executor.Delete(key);
        break;
      case Settings.OP_TYPE_VALUE:
        Slice value = Slice.DecodeLengthPrefix(op.GetData(), op.GetOffset() + 1 + key.GetLength() + Settings.UINT32_SIZE);
        executor.Put(key, value);
        break;
      default:
        throw new BadFormatException("write batch op_type not supported");
      }
    }
    
  }
}
