package com.yahoo.ycsb.db;

import java.util.List;

import com.google.common.collect.Lists;

public class NovaLSMClientStatsMain {

	public static void main(String[] args) {
		try {
			List<String> servers = Lists.newArrayList(args[0].split(","));
			NovaClient client = new NovaClient(servers, true);
			while (true) {
				boolean stop = true;
				for (int i = 0; i < servers.size(); i++) {
					int nl0tables = client.stats(i);
					System.out.println(String.format(
							"Waiting for L0 tables %d on %d", nl0tables, i));
					if (nl0tables > 0) {
						stop = false;
					}
				}
				if (!stop) {
					try {
						Thread.sleep(1000 * 10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
