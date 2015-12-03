package com.lilith.leveldb.table;

import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.api.Slice;
import java.io.DataOutputStream;


public class TableBuilder {
  
  /**
   * Create a builder that will store the contents of the table it is
   * building in *file. Does not close the file. It is up to the caller
   * to close the file after calling Finish().
   * @param options
   * @param writer
   */
  public TableBuilder(Options options, DataOutputStream writer) {
    
  }
  
  /**
   * Add key, value pair to the table being constructed. 
   * @param key
   * @param value
   */
  public void Add(Slice key, Slice value) {
    
  }
  
  /**
   * Flush any buffered key/value pairs to file.
   * Can be used to ensure that two adjacent entries never live in
   * the same data block. Most clients should not need to use this method.
   * Requires finish and abandon have not been called.
   */
  public void Flush() {
    
  }
  
  /**
   * Finish building the table. Stops using the file passed to the constructor after this
   * function returns.
   */
  public void Finish() {
    
  }
  
  /**
   * Indicate that the contents of the builder should be abandoned. Stops using the file passed to
   * the constructor after this function returns. If the caller is not going to call finish(). it
   * must call abandon before destroying this builder.
   */
  public void Abandon() {
    
  }
  
  /**
   * Number of calls to add() so far.
   * @return
   */
  public int NumEntries() {
    return 0;
  }
  
  /**
   * Size of the file generated so far. If invoked after a successful Finish() call, returns the size
   * of the final generated file.
   * @return
   */
  public int FileSize() {
    return 0;
  }
}
