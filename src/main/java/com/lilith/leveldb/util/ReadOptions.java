package com.lilith.leveldb.util;

import com.lilith.leveldb.impl.SnapShot;

public class ReadOptions {
  // If true, all data read from underlying storage will be verified against
  // corresponding checksums.
  public boolean verify_checksums = false;
  
  // Should the data read for this this iteration be cached in memory?
  public boolean fill_cache = true;
  
  // If 'snapshot' is not null, read as of the supplied snapshot (which must belong
  // to the DB that is being read and which must not have been released). If 'snapshot'
  // is null, use an implicit snapshot of the state at the beginning of this read operation.
  public SnapShot snapshot = null;
}


