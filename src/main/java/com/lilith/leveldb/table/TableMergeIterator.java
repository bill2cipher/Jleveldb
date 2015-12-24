package com.lilith.leveldb.table;

import java.io.IOException;
import java.util.ArrayList;

import com.lilith.leveldb.api.Comparator;
import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.exceptions.BadFormatException;

/**
 * An Iterator that provided the union of the data in children[0, n - 1].
 * The result does no duplicate suppression. I.e., if a particular key is
 * present in K child iterators, it will be yield K times. 
 */
public class TableMergeIterator {
  
  private Comparator comparator = null;
  private ArrayList<TableIterator> children = null;
  private TableIterator current = null;
  private int direction = 0;
  private static int FORWARD = 1;
  private static int BACKWARD = 2;
  
  public TableMergeIterator(Comparator comparator, ArrayList<TableIterator> children) {
    this.comparator = comparator;
    this.children = children;
    this.current = null;
    this.direction = FORWARD;
  }
  
  public boolean Valid() {
    return current != null;
  }
  
  public void SeekToFirst() throws BadFormatException, IOException {
    for (int i = 0; i < children.size(); i++) {
      children.get(i).SeekToFirst();
    }
    FindSmallest();
    direction = FORWARD;
  }
  
  public void SeekToLast() throws BadFormatException, IOException {
    for (int i = 0; i < children.size(); i++) {
      children.get(i).SeekToLast();
    }
    FindLargest();
    direction = BACKWARD;
  }
  
  public void Seek(Slice key) throws BadFormatException, IOException {
    for (int i = 0; i < children.size(); i++) {
      children.get(i).Seek(key);
    }
    FindSmallest();
    direction = FORWARD;
  }
  
  public void Next() throws BadFormatException, IOException {
    if (direction != FORWARD) {
      for (int i = 0; i < children.size(); i++) {
        TableIterator child = children.get(i);
        if (child == current) continue;
        child.Seek(Key());
        if (child.Valid() && comparator.Compare(Key(), child.Key()) == 0) child.Next();
      }
      direction = FORWARD;
    }
    
    current.Next();
    FindSmallest();
  }
  
  public void Prev() throws BadFormatException, IOException {
    if (direction != BACKWARD) {
      for (int i = 0; i < children.size(); i++) {
        TableIterator child = children.get(i);
        if (child == current) continue;
        child.Seek(Key());
        if (child.Valid()) child.Prev();
        else child.SeekToLast();
      }
    }
    current.Prev();
    FindLargest();
  }
  
  public Slice Key() {
    return current.Key();
  }
  
  public Slice value() {
    return current.Value();
  }
  
  private void FindSmallest() {
    TableIterator smallest = null;
    for (int i = 0; i < children.size(); i++) {
      TableIterator child = children.get(i);
      if (!child.Valid()) continue;
      if (smallest == null) smallest = child;
      else if (comparator.Compare(child.Key(), smallest.Key()) < 0) {
        smallest = child;
      }
    }
    current = smallest;
  }
  
  private void FindLargest() {
    TableIterator largest = null;
    for (int i = 0; i < children.size(); i++) {
      TableIterator child = children.get(i);
      if (!child.Valid()) continue;
      if (largest == null) largest = child;
      else if (comparator.Compare(child.Key(), largest.Key()) > 0) {
        largest = child;
      }
    }
    current = largest;
  }
}
