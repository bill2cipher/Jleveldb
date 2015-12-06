package com.lilith.leveldb.util;

/**
 * Class used to help binary operation on bytes.
 * @author Administrator
 *
 */
public class BinaryUtil {
  
  /**
   * Encode the value which is int32 into buffer, starting at index offset, in little-endien
   */
  public static boolean PutVarint32(byte[] buffer, int offset, int value) { 
    return PutVarint(Settings.UINT32_SIZE, buffer, offset, value);
  }
  
  /**
   * Encode the value which is int64 into buffer, starting at index offset, in little-endien
   */
  public static boolean PutVarint64(byte[] buffer, int offset, long value) {
    return PutVarint(Settings.UINT64_SIZE, buffer, offset, value);
  }
  
  
  private static boolean PutVarint(int bytes, byte[] buffer, int offset, long value) {
    if (buffer.length - offset < bytes) return false;
    for (int i = 0; i < bytes; i++) {
      buffer[offset + i] = (byte) (0xFF & value);
      value = value >> Settings.BYTE_SIZE;
    }
    return true;
  }

  /**
   * Decode the value which is int32 from buffer, starting at offset, in little-endien
   */
  public static int DecodeVarint32(byte[] value, int offset) {
    return (int)(0XFFFFFFFF & DecodeVarint(Settings.UINT32_SIZE, value, offset));
  }
  
  /**
   * Decode the value which is int64 from buffer, starting at offset, in little-endien
   */  
  public static long DecodeVarint64(byte[] value, int offset) {
    return DecodeVarint(Settings.UINT64_SIZE, value, offset);
  }
  
  private static long DecodeVarint(int bytes, byte[] value, int offset) {
    int result = 0;
    for (int i = offset; i < bytes; i++) {
      result |= value[i];
    }
    return result;
  }
  
  public static void CopyBytes(byte[] src, int src_offset, int src_size, byte[] dst, int dst_offset) {
    for (int i = 0, j = 0; i < src_size; i++, j++) {
      dst[j + dst_offset] = src[i + src_offset];
    }
  }
  
  public static int CompareBytes(byte[] first, int f_offset, int f_size, byte[] second, int s_offset, int s_size) {
    int min_len = f_size > s_size ? s_size : f_size;
    for (int i = 0; i < min_len; i++) {
      if (first[i + f_offset] == second[i + s_offset]) continue;
      if (first[i + f_offset] < second[i + s_offset]) return -1;
      return 1;
    }
    if (f_size == s_size) return 0;
    if (f_size <  s_size) return -1;
    return 1;
  }
}
