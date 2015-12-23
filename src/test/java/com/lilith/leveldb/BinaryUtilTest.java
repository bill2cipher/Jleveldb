package com.lilith.leveldb;

import org.junit.Test;
import org.junit.Assert;

import com.lilith.leveldb.util.BinaryUtil;

public class BinaryUtilTest {
  
  @Test
  public void TestEncode64() {
    byte[] buffer = new byte[24];
    BinaryUtil.PutVarint64(buffer, 0, 4290304832L);
    long rslt = BinaryUtil.DecodeVarint64(buffer, 0);
    Assert.assertTrue(rslt == 4290304832L);
  }
  
  @Test
  public void TestEncode32() {
    byte[] buffer = new byte[24];
    BinaryUtil.PutVarint32(buffer, 0, 42903048);
    long rslt = BinaryUtil.DecodeVarint32(buffer, 0);
    Assert.assertTrue(rslt == 42903048);
  }
  
  @Test
  public void TestEncode16() {
    byte[] buffer = new byte[24];
    BinaryUtil.PutVarint16(buffer, 0, 65520);
    long rslt = BinaryUtil.DecodeVarint16(buffer, 0);
    Assert.assertTrue(rslt == 65520);
  }

}
