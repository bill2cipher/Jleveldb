package com.lilith.leveldb.memtable;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import com.lilith.leveldb.util.Random;


public class SkipList<Key, Cmp extends Comparator<Key>> {
  private final Node header;
  private final Cmp compare;
  private AtomicInteger max_height;
  private Random random = null;
  private final static int MAX_HEIGHT = 12;
  private final static int SEED = 0XDEADBEEF;
  
  
  /**
   * Create a new skiplist object that will use 'cmp' for comparing keys,
   * and will allocate memory using arena. Objects allocated in the arena
   * must remain allocated for the lifetime of the skiplist object.
   * @param cmp
   */
  public SkipList(Cmp cmp) {
    random = new Random(SEED);
    header = NewNode(null, MAX_HEIGHT);
    compare = cmp;
    max_height.set(1);
    for (int i = 0; i < MAX_HEIGHT; i++) header.SetNext(i, null);
  }
  
  /**
   * Must external synchronized
   * @param key
   */
  public void Insert(Key key) {
    Node[] prev = (SkipList<Key, Cmp>.Node[]) new Object[MAX_HEIGHT];
    Node x = FindGreaterOrEqual(key, prev);
    
    int height = RandomHeight();
    if (height > GetMaxHeight()) {
      for (int i = GetMaxHeight(); i < height; i++) {
        prev[i] = header;
      }
          
      // it's ok to mutate max_height without any synchronization
      // with concurrent readers. A concurrent reader that observes
      // the new value of max_height will see either
      max_height.set(height);
    }
    x = NewNode(key, height);
    for (int i = 0; i < height; i++) {
      x.SetNext(i,  prev[i].Next(i));
      prev[i].SetNext(i, x);
    }
  }
  
  public boolean Contains(Key key) {
    Node x = FindGreaterOrEqual(key, null);
    if (x != null && Equal(key, x.key)) return true;
    else return false;
  }
  
  private int GetMaxHeight() {
    return max_height.get();
  }
  
  private Node NewNode(Key key, int height) {
    return new Node(key, height);
  }
  
  private int RandomHeight() {
    int Branching = 4;
    int height = 1;
    while (height < MAX_HEIGHT && (random.Next() % Branching) == 0) height++;
    return height;
  }
  
  private boolean Equal(Key a, Key b) {
    return compare.compare(a, b) == 0;
  }
  
  // return true if key is greater than the data stored in "n"
  // null is considered infinite
  private boolean KeyIsAfterNode(Key key, Node n) {
    return (n != null) && (compare.compare(n.key, key) < 0);
  }
  
  // return the earliest node that comes at or after key.
  // return null if there's no such node.
  // if prev is non-null, fills prev[level] with pointer to
  // previous node at level for every level in [0..max_height - 1].
  private Node FindGreaterOrEqual(Key key, Node[] prev) {
    Node x = header;
    int level = GetMaxHeight() - 1;
    while (true) {
      Node next = x.NextSync(level);
      if (KeyIsAfterNode(key, next)) {
        x = next;
      } else {
        if (prev != null) prev[level] = x;
        if (level == 0) return next;
        else level--;
      }
    }
  }
  
  // return the lastest node with a key < key.
  // return head if there's no such node.
  private Node FindLessThan(Key key) {
    Node x = header;
    int level = GetMaxHeight() - 1;
    while (true) {
      Node next = x.NextSync(level);
      if (next == null || compare.compare(next.key, key) >= 0) {
        if (level == 0) return x;
        else level--;
      } else {
        x = next;
      }
    }
  }
  
  // return the last node in the list.
  // return head if list is empty.
  private Node FindLast() {
    Node x = header;
    int level = GetMaxHeight() - 1;
    while (true) {
      Node next = x.NextSync(level);
      if (next == null) level--;
      if (level < 0) return x;
      x = next;
    }
  }
  
  public class Iterator {
    private Node node = null;
    
        
    // return true iff the iterator is positioned at a valid node.
    public boolean Valid() {
      return node != null;
    }
    
    // return the key at the current position.
    public Key Key() {
      return node.key;
    }
    
    // advances to the next position
    public void Next() {
      node = node.NextSync(0);
    }
    
    // advances to the previous position
    public void Prev() {
      node = FindLessThan(node.key);
      if (node == header) node = null;
    }
    
    // advance to the first entry with a key > target
    public void Seek(Key target) {
      node = FindGreaterOrEqual(target, null);
    }
    
    // Position at the first entry in the list.
    public void SeekToFirst() {
      node = header.NextSync(0);
    }
    
    // Position at the last entry in the list
    public void SeekToLast() {
      node = FindLast();
      if (node == header) node = null;
    }
  }
  
  
  
  class Node {
    private Key key;
    private Node[] next = null;
    
    public Node(Key key, int height) {
      this.key = key;
      next = (SkipList<Key, Cmp>.Node[]) new Object[height];
    }
    
    public Node(Key key) {
      this(key, 1);
    }
    
    // accessors/mutators for links. Wrapped in methods so we can
    // make it multi-thread safe.
    public Node Next(int n) {
      return next[n];
    }
    
    public Node NextSync(int n) {
    	synchronized(next[n]) {
    		return next[n];
    	}
    }
    
    public void SetNext(int n, Node node) {
      next[n] = node;
    }
    
    public void SetNextSync(int n, Node node) {
    	synchronized(next[n]) {
    		next[n] = node;
    	}
    }
  }
 
}

