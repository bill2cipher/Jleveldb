package com.lilith.leveldb.exceptions;

/**
 * Decode failed from a byte[] into an object.
 * @author Administrator
 *
 */
public class DecodeFailedException extends Exception {
  
  public DecodeFailedException(String msg) {
    super(msg);
  }
}
