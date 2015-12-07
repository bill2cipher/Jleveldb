package com.lilith.leveldb.exceptions;

/**
 * Decode failed from a byte[] into an object.
 * @author Administrator
 *
 */
public class DecodeFailedException extends Exception {
  
  private static final long serialVersionUID = -5517477927235659389L;

  public DecodeFailedException(String msg) {
    super(msg);
  }
}
