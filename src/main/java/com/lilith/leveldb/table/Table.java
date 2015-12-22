package com.lilith.leveldb.table;

import java.io.DataInputStream;
import java.io.IOException;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.api.SliceComparator;
import com.lilith.leveldb.exceptions.BadFormatException;
import com.lilith.leveldb.exceptions.DecodeFailedException;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.Settings;

/**
 * A table is a sorted map from strings to strings. Tables are immutable and persistent.
 * A table may be safely accessed from multiple threads without external synchronization.
 */
public class Table {
  
  public static final long TableMagicNumber = 0XDB4775248B80FB57L;
  private TableRep rep = null;
  
  public static class TableRep {
    public DataInputStream reader;
    public BlockHandle metaindex_handle;
    public Block index_block;
    
    public long cache_id;
    public FilterBlockReader filter;
    public FilterPolicy filter_policy;
    public Slice filter_data;
    
    public Options options;
    public ReadOptions read_opt;
  }
  
  // Build a table with the given file.
  public static Table Open(DataInputStream reader, int file_size, Options options)
      throws IOException, DecodeFailedException, BadFormatException {
    if (file_size < Footer.EncodedLength) return null;
    byte[] footer_data = new byte[Footer.EncodedLength];
    reader.read(footer_data, file_size - Footer.EncodedLength, Footer.EncodedLength);
    Footer footer = new Footer();
    footer.DecodeFrom(new Slice(footer_data));
    
    ReadOptions opt = new ReadOptions();
    if (options.paranoid_checks) opt.verify_checksums = true;
    else opt.verify_checksums = false;
    
    BlockContent content = new BlockContent(Slice.EmptySlice, true, true);
    Block.ReadBlock(reader, opt, footer.GetIndexHandle(), content);
    Block index_block = new Block(content);
    
    
    TableRep rep = new TableRep();
    rep.index_block = index_block;
    rep.reader = reader;
    rep.metaindex_handle = footer.GetMetaIndexHandle();
    rep.options = options;
    rep.cache_id = (options.block_cache == null) ? options.block_cache.NewId() : 0;
    rep.filter_data = Slice.EmptySlice;
    rep.filter = null;
    rep.read_opt = opt;
    
    Table table = new Table(rep);
    table.ReadMeta(footer);
    return table;
  }
  
  public Table(TableRep rep) {
    this.rep = rep;
  }
  
  /**
   * Returns a new iterator over the table contents. The result of TableIterator is initially invalid.
   * Caller must call one of seek methods on the iterator before using it.
   */
  public TableIterator TableIterator(ReadOptions options) {
    return null;
  }
  

  
  private void ReadMeta(Footer footer) throws IOException, BadFormatException {
    if (rep.options.filter_policy == null) return;
    
    BlockContent content = new BlockContent();
    Block.ReadBlock(rep.reader, rep.read_opt, footer.GetMetaIndexHandle(), content);
    Block meta_block = new Block(content);
    
    SliceComparator comp = new SliceComparator();
    BlockIterator block_iter = meta_block.Iterator(comp);
    FilterPolicy filter = rep.options.filter_policy;
    Slice filter_name = new Slice(filter.toString().getBytes());
    block_iter.Seek(filter_name);
    
    if (block_iter.Valid() && comp.Compare(block_iter.Key(), filter_name) == 0) {
      ReadFilter(block_iter.Value());
      rep.filter_policy = filter;
    }
  }
  
  private void ReadFilter(Slice filter) throws IOException, BadFormatException {
    BlockHandle filter_handle = new BlockHandle();
    filter_handle.DecodeFrom(filter.GetData(), filter.GetOffset());
    BlockContent content = new BlockContent();
    
    Block.ReadBlock(rep.reader, rep.read_opt, filter_handle, content);
    rep.filter_data = content.GetData();
    rep.filter = new FilterBlockReader(rep.options.filter_policy, content.GetData());
  }
  
  /**
   * Convert an index iterator value into an iterator over the contents of the corresponding block.
   * @throws BadFormatException 
   * @throws IOException 
   */
  private BlockIterator BlockReader(ReadOptions options, Slice index_value) throws IOException, BadFormatException {
    Cache<Slice, Block> block_cache = rep.options.block_cache;
    Block block = null;
    
    BlockHandle handle = new BlockHandle();
    handle.DecodeFrom(index_value.GetData(), index_value.GetOffset());
    
    BlockContent content = new BlockContent();
    if (block_cache != null) {
      byte[] cache_key_buffer = new byte[16];
      BinaryUtil.PutVarint64(cache_key_buffer, 0, rep.cache_id);
      BinaryUtil.PutVarint64(cache_key_buffer, Settings.UINT64_SIZE, handle.GetOffset());
      Slice key = new Slice(cache_key_buffer);
      block = block_cache.Lookup(key, key.hashCode());
      if (block == null) {
        Block.ReadBlock(rep.reader, options, handle, content);
        block = new Block(content);
        if (options.fill_cache) block_cache.Insert(key, block, block.Size(), key.hashCode());
      }
    } else {
      Block.ReadBlock(rep.reader, options, handle, content);
      block = new Block(content);
    }
    
    return block.Iterator(rep.options.cmp);
  }

  /**
   * Given a key, return an approximate byte offset in the file where the data for that key begins.
   * The returned value is in term of file bytes, and so includes effects like compression of the
   * underlying data.
   * @throws BadFormatException 
   */
  public int ApproximateOffsetOf(Slice key) throws BadFormatException {
    BlockIterator index_iter = rep.index_block.Iterator(rep.options.cmp);
    index_iter.Seek(key);
    
    if (!index_iter.Valid()) return -1;
    BlockHandle handle = new BlockHandle();
    Slice handle_value = index_iter.Value();
    handle.DecodeFrom(handle_value.GetData(), handle_value.GetOffset());
    return handle.GetOffset();
  }

  private Slice InternalGet(ReadOptions options, Slice key) throws BadFormatException, IOException {
    BlockIterator index_iter = rep.index_block.Iterator(rep.options.cmp);
    index_iter.Seek(key);
    
    if (!index_iter.Valid()) return null;
    Slice handle_value = index_iter.Value();
    FilterBlockReader filter = rep.filter;
    BlockHandle handle = new BlockHandle();
    handle.DecodeFrom(handle_value.GetData(), handle_value.GetOffset());
    
    if (filter != null && !filter.KeyMayMatch(handle.GetOffset(), key));
    BlockIterator block_iter = BlockReader(options, handle_value);
    block_iter.Seek(key);
    if (block_iter.Valid()) return block_iter.Value();
    return null;
  }
}


