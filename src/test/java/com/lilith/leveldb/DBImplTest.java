package com.lilith.leveldb;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.*;

import com.lilith.leveldb.api.LevelDB;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.Options;
import com.lilith.leveldb.util.ReadOptions;
import com.lilith.leveldb.util.WriteOptions;



public class DBImplTest {
  private static LevelDB db = null;
  
  @BeforeClass
  public static void initdb() {
    try {
      db = LevelDB.Open(new Options(), "leveldbtest");
    } catch (Exception exp) {
      exp.printStackTrace();
      fail("exception occurs while creating database");
    }
  }
  
  @AfterClass
  public static void destroydb() {
    db.CloseDB();
    File db = new File("leveldbtest");
    File[] child = db.listFiles();
    for (int i = 0; i < child.length; i++) {
      child[i].delete();
    }
    db.delete();
  }

  @Ignore
  @Test
  public void OpenDBTest() {
    try {
      Slice key = new Slice("key".getBytes());
      Slice value = new Slice("value".getBytes());
      db.Put(new WriteOptions(), key, value);
    } catch (Exception exp) {
      exp.printStackTrace();
      fail("exception occurs while creating database");
    }
  }
  
  @Test
  public void PutDBTest() {
    try {
      ReadOptions read = new ReadOptions();
      for (int i = 0; i < 10000; i++) {
        System.out.println(i);
        db.Put(new WriteOptions(), Util.str2slice("key" + i), Util.str2slice("value" + i));
        Slice getVal = db.Get(read, Util.str2slice("key" + i));
        assertTrue(getVal != null);
        assertTrue(getVal.compareTo(Util.str2slice("value" + i)) == 0);
      }
    } catch (Exception exp) {
      exp.printStackTrace();
      fail("exception occurs while reading database");
    }
  }
  
  @Ignore
  @Test
  public void DeleteTest() {
    try {
      Slice key = new Slice("key".getBytes());
      db.Delete(new WriteOptions(), key);
      
      Slice getVal = db.Get(new ReadOptions(), key);
      assertEquals(getVal, null);
    } catch (Exception exp) {
      exp.printStackTrace();
      fail("exception occurs while reading database");
    }
  }
}
