package com.lilith.leveldb.log;

/**
 * The log file contents are sequence of 32KB blocks. The only exception is that the tail of
 * the file may contain a partial block.
 * 
 * Each block consists of a sequence of records:
 * block  := record* trailer?
 * record :=
 *   checksum: uint32   // crc32c of type and data[];
 *   length:   uint16   // 
 *   type  :   uint8    //One of FULL, FIRST, MIDDLE, LAST
 *   data  :   uint8[length]
 *   
 * A record never starts within the last six bytes of a block(since it won't fit).
 * Any leftover bytes here form the trailer, which must consist entirely of zero bytes
 * and must be skipped by readers.
 * 
 * Aside: if exactly seven bytes are left in the current block, and a new non-zero length
 * record is added, the writer must emit a FIRST record (which contains zero bytes of user
 * data) to fill up the trailing seven bytes of the block and then emit all of the user
 * data in subsequent blocks.
 *
 * More types may be added in the future. Some readers may skip record types they do not
 * understand, others report that some data was skipped.
 * FULL   = 1
 * FIRST  = 2
 * MIDDLE = 3
 * LAST   = 4
 * 
 * The FULL record contains the contents of an entire user record.
 * FIRST, MIDDLE, LAST are types used for user records that have been split into multiple
 * fragments (typically because of block boundaries). FIRST is the type of the first
 * fragment of a user record, LAST is the type of the last fragment of a user record,
 * and MIDDLE is the type of all interior fragments of a user record.
 * 
 * The benefits over the record format:
 * 1. Do not need any heuristics for resyncing;
 * 2. Split at approximate boundaries is simple;
 * 3. Do not need extra buffering for large records;
 * 
 * The downsides compared to record format:
 * 1. No packing of tiny records;
 * 2. No compression.
 */
public class LogFormat {
  public static final byte FULL   = 0;
  public static final byte FIRST  = 1;
  public static final byte MIDDLE = 2;
  public static final byte LAST   = 3;
  
  public static final byte MAX_RECORD_TYPE = LAST;
  
  public static final int BLOCK_SIZE = 32768;
  
  public static final int HEADER_SIZE = 4 + 2 + 1;
  
  public static final byte[] ZERO_BYTES = {0X00, 0X00, 0X00, 0X00, 0X00, 0X00, 0X00, 0X00};
}
