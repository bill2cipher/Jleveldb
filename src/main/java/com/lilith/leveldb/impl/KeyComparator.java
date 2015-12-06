package com.lilith.leveldb.impl;

import java.util.Comparator;

import com.lilith.leveldb.api.Slice;
import com.lilith.leveldb.util.BinaryUtil;
import com.lilith.leveldb.util.Settings;

public class KeyComparator implements Comparator<Slice> {

	public int compare(Slice a, Slice b) {
		int a_size = BinaryUtil.DecodeVarint32(a.GetData(), a.GetOffset());
		int b_size = BinaryUtil.DecodeVarint32(b.GetData(), b.GetOffset());
		return BinaryUtil.CompareBytes(a.GetData(), a.GetOffset() + Settings.UINT32_SIZE, a_size - Settings.UINT64_SIZE
		                             , b.GetData(), b.GetOffset() + Settings.UINT32_SIZE, b_size - Settings.UINT64_SIZE);
	}
}
