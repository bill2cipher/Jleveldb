package com.lilith.leveldb.impl;

import java.util.Comparator;

import com.lilith.leveldb.api.Slice;

public class KeyComparator implements Comparator<Slice> {

	public int compare(Slice a, Slice b) {
		return a.compareTo(b);
	}

}
