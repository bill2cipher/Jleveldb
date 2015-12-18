package com.lilith.leveldb.api;

/**
 * A comparator object provides a total order across slices that are used
 * as keys in an sstable or a database. A comparator implementation
 * must be thread-safe since leveldb may invoke its methods concurrently
 * form multiple threads.
 */
public abstract class Comparator {
  
  /**
   * Three-way comparison.
   */
  public abstract int Compare(Slice fval, Slice sval);
  
  /**
   * The name of the comparator.
   */
  public abstract String Name();
  
  /**
   * Advanced functions: these are used to reduce the space requirements for
   * internal data structures like index blocks.
   */
  public abstract Slice FindShortestSeparator(Slice start, Slice limit);
  
  /**
   * return a slice >= key
   */
  public abstract Slice FindShortestSuccessor(Slice key);
}
