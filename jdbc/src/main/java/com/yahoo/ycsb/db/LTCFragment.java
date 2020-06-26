package com.yahoo.ycsb.db;

import java.util.List;

import com.google.common.collect.Lists;

public class LTCFragment {
	public long startKey;
	public long endKey;
	public int ltcServerId;
	public int dbId;
	public List<Integer> stocLogReplicaServerIds = Lists.newArrayList();

	@Override
	public String toString() {
		return "Fragment [start=" + startKey + ", end=" + endKey
				+ ", serverIds=" + ltcServerId + ", dcServerIds=" + stocLogReplicaServerIds
				+ "]";
	}

}
