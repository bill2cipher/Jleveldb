package com.lilith.leveldb.table;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.memtable.MemIterator;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.FileName;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.Settings;
import com.lilith.leveldb.version.FileMetaData;


public class TableBuilder {
  
  private class Rep {
    public Options options;
    public Options index_block_options;
    public DataOutputStream writer;
    public int offset;
    public BlockBuilder data_block;
    public BlockBuilder index_block;
    
    public Slice last_key;
    public int num_entries;
    public boolean closed;
    public FilterBlockBuilder filter_block;
    
    public boolean pending_index_entry;
    public BlockHandle pending_handle; // handle to add to index block
    public Slice compressed_output;
    
    public Rep(Options opt, DataOutputStream file) {
      options = opt.clone();
      index_block_options = opt.clone();
      index_block_options.block_restart_interval = 1;
      writer = file;
      offset = 0;
      data_block = new BlockBuilder(options);
      index_block = new BlockBuilder(index_block_options);
      
      last_key = Slice.EmptySlice;
      num_entries = 0;
      closed = false;
      pending_index_entry = false;
      pending_handle = new BlockHandle();
      compressed_output = Slice.EmptySlice;
      
      if (opt.filter_policy == null)
        filter_block = null;
      else filter_block = new FilterBlockBuilder(opt.filter_policy);
    }
  }
  
  private Rep rep = null;
  private CRC32 crc32 = null;
  
  /**
   * Create a builder that will store the contents of the table it is
   * building in *file. Does not close the file. It is up to the caller
   * to close the file after calling Finish().
   * @param options
   * @param writer
   */
  public TableBuilder(Options options, DataOutputStream writer) {
    rep = new Rep(options, writer);
    crc32 = new CRC32();
    if (rep.filter_block != null) rep.filter_block.StartBlock(0);
  }
  
  /**
   * Change the options used by this builder. Note: Only some of the option fields can
   * be changed after construction. If a field is not allowed to change dynamically and
   * its value in the structure passed to the constructor is different from its value
   * in the structure passed to this method, this method will return an error without
   * changing fields.
   */
  public boolean ChangeOptions(Options opt) {
    if (rep.options.cmp != opt.cmp) return false;
    rep.options = opt.clone();
    rep.index_block_options = opt.clone();
    rep.index_block_options.block_restart_interval = 1;
    return true;    
  }
  
  /**
   * Add key, value pair to the table being constructed. 
   * @param key
   * @param value
   * @throws IOException 
   */
  public boolean Add(Slice key, Slice value) throws IOException {
    if (rep.closed) return false;
    
    if (rep.pending_index_entry) {
      byte[] buffer = new byte[rep.pending_handle.GetSize()];
      rep.options.cmp.FindShortestSeparator(rep.last_key, key);
      rep.pending_handle.EncodeTo(buffer, 0);
      rep.index_block.Add(rep.last_key, new Slice(buffer));
      rep.pending_index_entry = false;
    }
    
    if (rep.filter_block != null) {
      rep.filter_block.AddKey(key);
    }
    
    rep.last_key = key;
    rep.num_entries++;
    rep.data_block.Add(key, value);
    
    int size = rep.data_block.CurrentSizeEstimate();
    if (size >= rep.options.block_size) Flush();
    return true;
  }
  
  /**
   * Flush any buffered key/value pairs to file.
   * Can be used to ensure that two adjacent entries never live in
   * the same data block. Most clients should not need to use this method.
   * Requires finish and abandon have not been called.
   * @throws IOException 
   */
  public void Flush() throws IOException {
    if (rep.data_block.Empty()) return;
    WriteBlock(rep.data_block, rep.pending_handle);
    rep.pending_index_entry = true;
    rep.writer.flush();
    
    if (rep.filter_block != null)
      rep.filter_block.StartBlock(rep.offset);
  }
  
  /**
   * Finish building the table. Stops using the file passed to the constructor after this
   * function returns.
   * @throws IOException 
   */
  public void Finish() throws IOException {
    Flush();
    if (rep.closed) return;
    rep.closed = true;
    
    BlockHandle filter_block_handle = new BlockHandle();
    BlockHandle metaindex_block_handle = new BlockHandle();
    BlockHandle index_block_handle = new BlockHandle();
    
    // write filter block
    if (rep.filter_block != null) {
      WriteRawBlock(rep.filter_block.Finish(), Settings.NoCompression, filter_block_handle);
    }
    
    // write metaindex block
    BlockBuilder meta_index_block = new BlockBuilder(rep.options);
    if (rep.filter_block != null) {
      Slice key = new Slice(rep.filter_block.toString().getBytes());
      byte[] metaindex_buffer = new byte[filter_block_handle.GetSize()];
      
      filter_block_handle.EncodeTo(metaindex_buffer, 0);
      meta_index_block.Add(key, new Slice(metaindex_buffer));
    }
    WriteBlock(meta_index_block, metaindex_block_handle);
    
    // write index block
    if (rep.pending_index_entry) {
      rep.options.cmp.FindShortestSuccessor(rep.last_key);
      
      byte[] index_handle = new byte[rep.pending_handle.GetSize()];
      rep.pending_handle.EncodeTo(index_handle, 0);
      rep.index_block.Add(rep.last_key, new Slice(index_handle));
      rep.pending_index_entry = false;
    }
    WriteBlock(rep.index_block, index_block_handle);
    
    // write footer
    Footer footer = new Footer();
    footer.SetMetaIndexHandle(metaindex_block_handle);
    footer.SetIndexHandle(index_block_handle);
    byte[] foot_buffer = new byte[Footer.EncodedLength];
    footer.EncodeTo(foot_buffer, 0);
    rep.writer.write(foot_buffer, 0, foot_buffer.length);
    rep.offset += foot_buffer.length;
  }
  
  /**
   * Indicate that the contents of the builder should be abandoned. Stops using the file passed to
   * the constructor after this function returns. If the caller is not going to call finish(). it
   * must call abandon before destroying this builder.
   */
  public void Abandon() {
    rep.closed = true;
  }
  
  /**
   * Number of calls to add() so far.
   * @return
   */
  public int NumEntries() {
    return rep.num_entries;
  }
  
  /**
   * Size of the file generated so far. If invoked after a successful Finish() call, returns the size
   * of the final generated file.
   * @return
   */
  public int FileSize() {
    return rep.offset;
  }
  
  /**
   * File format contains a sequence of blocks where each block has:
   * block_data : uint8[n]
   * type : uint8
   * crc  : uint32
   * @throws IOException 
   */
  private void WriteBlock(BlockBuilder block, BlockHandle handle) throws IOException {
    Slice raw = block.Finish();
    Slice block_content = null;
    int compress_type = rep.options.compression_type;
    switch(compress_type) {
    case Settings.NoCompression:
      block_content = raw;
      break;
    case Settings.SnappyCompression:
      block_content = raw;
      compress_type = Settings.NoCompression;
      break;
    }
    WriteRawBlock(block_content, compress_type, handle);
    rep.compressed_output = Slice.EmptySlice;
    block.Reset();
  }
  
  private void WriteRawBlock(Slice data, int compress_type, BlockHandle handle) throws IOException {
    handle.SetOffset(rep.offset);
    handle.SetSize(data.GetLength());
    rep.writer.write(data.GetData(), data.GetOffset(), data.GetLength());
    
    byte[] trailer = new byte[Block.BlockTrailerSize];
    trailer[0] = (byte) (compress_type & 0xFF);
    crc32.update(data.GetData(), data.GetOffset(), data.GetLength());
    BinaryUtil.PutVarint32(trailer, 1, (int) crc32.getValue());
    rep.writer.write(trailer, 0, Block.BlockTrailerSize);
    
    rep.offset += data.GetLength() + Block.BlockTrailerSize;
  }
  
  /**
   * Build a Table file from the content of mem_iter. The generated file will be named according to
   * file_meta.number. On success, the rest of meta will be filled with meta data about the generated
   * table. If no data is present in mem_iter, file_meta.file_size will be set to zero, and no Table
   * file will be produced.
   * @throws IOException 
   */
  public static void BuildTable(String dbname, Options options, MemIterator mem_iter, FileMetaData file_meta) throws IOException {
    file_meta.file_size = 0;
    mem_iter.SeekToFirst();
    String table_name = FileName.TableFileName(dbname, file_meta.number);
    
    DataOutputStream writer = new DataOutputStream(new FileOutputStream(table_name));
    TableBuilder builder = new TableBuilder(options, writer);
    Slice internal_key = mem_iter.Key();
    file_meta.smallest.DecodeFrom(internal_key.GetData(), internal_key.GetOffset(), internal_key.GetLength());
    
    for (; mem_iter.Valide(); mem_iter.Next()) {
      internal_key = mem_iter.Key();
      file_meta.largest.DecodeFrom(internal_key.GetData(), internal_key.GetOffset(), internal_key.GetLength());
      builder.Add(internal_key, mem_iter.Value());
    }
    
    builder.Finish();
    file_meta.file_size = builder.FileSize();
    writer.close();
  }
}
