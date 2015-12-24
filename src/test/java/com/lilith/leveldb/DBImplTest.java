package com.lilith.leveldb;

import static org.junit.Assert.*;

import org.junit.*;

import com.lilith.leveldb.api.LevelDB;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.WriteOptions;



public class DBImplTest {

  @Test
  public void OpenDBTest() {
    try {
      LevelDB db = LevelDB.Open(new Options(), "leveldbtest");
      Slice key = new Slice("key".getBytes());
      Slice value = new Slice("value".getBytes());
      db.Put(new WriteOptions(), key, value);
    } catch (Exception exp) {
      exp.printStackTrace();
      fail("exception occurs while creating database");
    }
  }

}
