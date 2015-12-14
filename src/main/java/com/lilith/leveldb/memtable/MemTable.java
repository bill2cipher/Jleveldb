package com.lilith.leveldb.memtable;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;
import com.lilith.leveldb.version.InternalKeyComparator;
/**
 * Storing key/value pairs in the memory.
 * @author Administrator
 *
 */
public class MemTable {
  private SkipList<Slice, KeyComparator> table = null;
  private AtomicInteger size = null;
  private KeyComparator comp = null;
  
  /**
   * Comparator used to compare internal keys in memtable.
   */
  public class KeyComparator implements Comparator<Slice> {
    private InternalKeyComparator internal_comparator = null;
    public KeyComparator(InternalKeyComparator internal_comparator) {
      this.internal_comparator = internal_comparator;
    }
    
    public int compare(Slice fval, Slice sval) {
      Slice fval2 = Slice.GetLengthPrefix(fval);
      Slice sval2 = Slice.GetLengthPrefix(sval);
      return internal_comparator.compare(fval2, sval2);
    }
  }
  
  public MemTable(InternalKeyComparator cmp) {
    comp = new KeyComparator(cmp);
    table = new SkipList<Slice, KeyComparator>(comp);
    size = new AtomicInteger(0);
  }
  
  /**
   * Add an entry into memtble that maps key to value at the 
   * specified sequence number and with the specified type.
   * typically value will be empty if type == TypeDeletion.
   * format of an entry is concatenation of :
   * key_size   : internal key size
   * key bytes  : bytes[internal_key_size]
   * value_size : int32
   * value bytes: bytes[value_size]
   */
  public void Add(long sequence, byte op_type, Slice key, Slice value) {
    int key_size = key.GetLength();
    int value_size = value.GetLength();
    int internal_key_size = key_size + Settings.UINT64_SIZE;
    int offset = 0;
    byte[] buffer = new byte[Settings.UINT32_SIZE * 2 + internal_key_size + value_size];
    BinaryUtil.PutVarint32(buffer, offset, internal_key_size); offset += Settings.UINT32_SIZE;
    BinaryUtil.CopyBytes(key.GetData(), key.GetOffset(), key.GetLength(), buffer, offset); offset += key_size;
    BinaryUtil.PutVarint64(buffer, offset, (sequence << 8 | op_type)); offset += Settings.UINT64_SIZE;
    BinaryUtil.PutVarint32(buffer, offset, value_size); offset += Settings.UINT32_SIZE;
    BinaryUtil.CopyBytes(value.GetData(), value.GetOffset(), value.GetLength(), buffer, offset);
    table.Insert(new Slice(buffer));
    size.addAndGet(offset + value_size);
  }

  /**
   * return an estimate of the number of bytes data in use by this
   * data structure.
   * 
   * external synchronization to prevent simultaneous operation on the same table
   * @return
   */
  public int ApproximateMemoryUsage() {
    return size.get();
  }
  
  /**
   * Return an iterator that yields the contents of the memtable.
   * the caller must ensure underlying memtable remains live while
   * the returned iterator is live. The keys returned by this iterator are internal
   * keys encoded by AppendInternalKey in the db/format.h module
   * @return
   */
  public MemIterator Iterator() {
    return new MemIterator(table.new Iterator());
  }
  
  /**
   * if memtable contains a value for key, return it.
   * if memtable contains a deletion for key, return null.
   */
  public Slice Get(LookupKey lookup) {
    SkipList<Slice, KeyComparator>.Iterator iter = table.new Iterator();
    iter.Seek(lookup.MemTableKey());
    if (iter.Valid()) {
      Slice entry = iter.Key();
      int cmp_rslt = comp.compare(lookup.MemTableKey(), entry);
      if (cmp_rslt == 0) {
        int entry_tag_offset = DecodeInternalKeySize(entry) + Settings.UINT32_SIZE;
        byte op_type = (byte) (BinaryUtil.DecodeVarint64(entry.GetData(), entry_tag_offset) & 0XFF);
        switch (op_type) {
        case Settings.OP_TYPE_DELETE:
          return null;
        case Settings.OP_TYPE_VALUE:
          return Slice.DecodeLengthPrefix(entry.GetData(), entry_tag_offset + Settings.UINT64_SIZE);
        }
      }
    }
    return null;
  }
  
  private int DecodeInternalKeySize(Slice s) {
    return BinaryUtil.DecodeVarint32(s.GetData(), s.GetOffset()) - Settings.UINT64_SIZE;
  }
}
