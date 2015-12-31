package com.lilith.leveldb;

import static org.junit.Assert.*;

import java.util.Comparator;

import org.junit.*;

import com.lilith.leveldb.memtable.SkipList;

public class SkipListTest {
  private static class StrComparator implements Comparator<String> {
    public int compare(String f, String s) {
      return f.compareTo(s);
    }
  }
  
  public static SkipList<String, StrComparator> list = null;
  public static SkipList<String, StrComparator>.Iterator iter = null;
  
  @BeforeClass
  public static void InitSkipList() {
    list = new SkipList<String, StrComparator>(new StrComparator());
    iter = list.new Iterator();
  }
  
  
  @Test
  public void InsertTest() {
    list.Insert("key");
    assertTrue(list.Contains("key"));
    
    iter.SeekToFirst();
    assertTrue(iter.Valid());
    assertTrue(iter.Key() != null);
    assertTrue(iter.Key().compareTo("key") == 0);
  }
  
  @Test
  public void Find() {
    for (int i = 0; i < 1000; i++) {
      list.Insert("key" + String.valueOf(i));
    }
    
    for (int i = 0; i < 1000; i++) {
      assertTrue(list.Contains("key" + String.valueOf(i)));
    }
    
    iter.Seek("key10");
    assertTrue(iter.Valid());
    assertTrue(iter.Key().compareTo("key10") == 0);
    
    iter.SeekToLast();
    assertTrue(iter.Valid());
    assertTrue(iter.Key().compareTo("key999") == 0);
    
    iter.Seek("key88");
    assertTrue(iter.Valid());
    assertTrue(iter.Key().compareTo("key88") == 0);
    
    iter.Seek("home");
    assertFalse(!iter.Valid());
    
    iter.Seek("love");
    assertFalse(iter.Valid());
  }
}
