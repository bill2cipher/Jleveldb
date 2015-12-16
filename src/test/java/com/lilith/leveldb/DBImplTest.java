package com.lilith.leveldb;

import static org.junit.Assert.*;

import org.junit.*;

import com.lilith.leveldb.api.LevelDB;
import com.lilith.leveldb.util.Options;



public class DBImplTest {

  @Test
  public void OpenDBTest() {
    try {
      LevelDB.Open(new Options(), "leveldbtest");
    } catch (Exception exp) {
      exp.printStackTrace();
      fail("exception occurs while creating database");
    }
  }

}
