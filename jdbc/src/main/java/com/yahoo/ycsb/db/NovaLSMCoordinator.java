package com.yahoo.ycsb.db;

import java.util.List;

import com.google.common.collect.Lists;

public class NovaLSMCoordinator {

	public static void main(String[] args) throws Exception {
		List<String> servers = Lists.newArrayList(args[0].split(","));
		int timeout = Integer.parseInt(args[1]);
		int now = 0;
		while (true) {
			if (now == timeout) {
				break;
			}
			Thread.sleep(1000);
			now++;
		}

		NovaClient client = new NovaClient(servers, true);
		for (int i = 0; i < servers.size(); i++) {
			client.changeConfig(i);
			System.out.println("Change config");
		}
	}
}
