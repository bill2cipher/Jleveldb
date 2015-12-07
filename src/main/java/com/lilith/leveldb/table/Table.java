package com.lilith.leveldb.table;

import java.io.DataInputStream;
import java.io.IOException;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.DecodeFailedException;

/**
 * A table is a sorted map from strings to strings. Tables are immutable and persistent.
 * A table may be safely accessed from multiple threads without external synchronization.
 * @author Administrator
 *
 */
public class Table {
  
  public static final long TableMagicNumber = 0XDB4775248B80FB57L;
  
  private TableRep rep = null;
  
  // Build a table with the given file.
  public static Table Open(DataInputStream reader, int file_size) throws IOException, DecodeFailedException {
    if (file_size < Footer.EncodedLength) return null;
    byte[] footer_data = new byte[Footer.EncodedLength];
    reader.read(footer_data, file_size - Footer.EncodedLength, Footer.EncodedLength);
    Footer footer = new Footer();
    footer.DecodeFrom(new Slice(footer_data));
    
    BlockContent content = new BlockContent(Slice.EmptySlice, true, true);
    Block.ReadBlock(reader, footer.GetIndexHandle(), content);
    Block index_block = new Block(content);
    
    
    TableRep rep = new TableRep();
    rep.index_block = index_block;
    rep.reader = reader;
    rep.metaindex_handle = footer.GetMetaIndexHandle();
    
    Table table = new Table(rep);
    table.ReadMeta(footer);
    return table;
  }
  
  public Table(TableRep rep) {
    this.rep = rep;
  }
  
  private void ReadMeta(Footer footer) throws IOException {
    BlockContent content = new BlockContent();
    Block.ReadBlock(rep.reader, footer.GetMetaIndexHandle(), content);
    Block meta_block = new Block(content);
    
    BlockIterator block_iter = meta_block.Iterator();
  }
  
  
}

class TableRep {
  public DataInputStream reader;
  public BlockHandle metaindex_handle;
  public Block index_block;
}