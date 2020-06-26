package com.yahoo.ycsb.db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;

import com.google.common.collect.Lists;

public class ConfigurationUtil {
	public static List<LTCFragment> readConfig(String configPath) {
		List<LTCFragment> fragments = Lists.newArrayList();
		try {
			BufferedReader br = new BufferedReader(
					new FileReader(new File(configPath)));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] ems = line.split(",");
				LTCFragment frag = new LTCFragment();
				fragments.add(frag);
				frag.startKey = Long.parseLong(ems[0]);
				frag.endKey = Long.parseLong(ems[1]);
				frag.ltcServerId = Integer.parseInt(ems[2]);
				frag.dbId = Integer.parseInt(ems[3]);

				for (int i = 4; i < ems.length; i += 1) {
					frag.stocLogReplicaServerIds.add(Integer.parseInt(ems[i]));
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fragments;
	}

	public static void generateSharedCCRangePartitionConfig(String outputPath,
			int nRecords, int nLTCServers, int nLogReplicasPerRange,
			int nRangesPerServer) throws Exception {
		int tRanges = nRangesPerServer * nLTCServers;
		int nRecordsPerRange = nRecords / tRanges;
		List<LTCFragment> fragments = Lists.newArrayList();
		int startKey = 0;

		for (int i = 0; i < nLTCServers; i++) {
			for (int j = 0; j < nRangesPerServer; j++) {
				LTCFragment frag = new LTCFragment();
				frag.startKey = startKey;
				frag.endKey = startKey + nRecordsPerRange;
				frag.ltcServerId = i;
				frag.dbId = j;

				int serverId = i;
				for (int r = 0; r < nLogReplicasPerRange; r++) {
					frag.stocLogReplicaServerIds.add(serverId);
					serverId += 1;
					if (nLTCServers > 1) {
						serverId = serverId % nLTCServers;
					}
				}
				fragments.add(frag);
				startKey += nRecordsPerRange;
			}
		}

		// Last range.
		fragments.get(fragments.size() - 1).endKey = nRecords;

		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(String
				.format("%s/nova-shared-cc-nrecords-%d-nccservers-%d-nlogreplicas-%d-nranges-%d",
						outputPath, nRecords, nLTCServers, nLogReplicasPerRange,
						nRangesPerServer))));
		StringBuilder builder = new StringBuilder();
		for (LTCFragment frag : fragments) {
			builder.append(frag.startKey);
			builder.append(",");
			builder.append(frag.endKey);
			builder.append(",");
			builder.append(frag.ltcServerId);
			builder.append(",");
			builder.append(frag.dbId);
			builder.append(",");
			for (int i = 0; i < frag.stocLogReplicaServerIds.size(); i++) {
				builder.append(frag.stocLogReplicaServerIds.get(i));
				builder.append(",");
			}
			builder.deleteCharAt(builder.length() - 1);
			builder.append("\n");
		}
		bw.write(builder.toString());
		bw.flush();
		bw.close();
	}

	public static void generateCCRangePartitionConfig(String outputPath,
			int nRecords, int nCCServers, int nLogReplicasPerRange,
			int nRangesPerServer, int nDCServers) throws Exception {
		int tRanges = nRangesPerServer * nCCServers;
		int nRecordsPerRange = nRecords / tRanges;
		List<LTCFragment> fragments = Lists.newArrayList();
		int startKey = 0;
		int dcServerId = 0;

		for (int i = 0; i < nCCServers; i++) {
			for (int j = 0; j < nRangesPerServer; j++) {
				LTCFragment frag = new LTCFragment();
				frag.startKey = startKey;
				frag.endKey = startKey + nRecordsPerRange;
				frag.ltcServerId = i;
				frag.dbId = j;

				for (int r = 0; r < nLogReplicasPerRange; r++) {
					frag.stocLogReplicaServerIds.add(dcServerId + nCCServers);
					dcServerId += 1;
					dcServerId = dcServerId % nDCServers;
				}
				fragments.add(frag);
				startKey += nRecordsPerRange;
			}
		}

		// Last range.
		fragments.get(fragments.size() - 1).endKey = nRecords;

		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(String
				.format("%s/nova-cc-nrecords-%d-nccservers-%d-ndcservers-%d-nlogreplicas-%d-nranges-%d",
						outputPath, nRecords, nCCServers, nDCServers,
						nLogReplicasPerRange, nRangesPerServer))));
		StringBuilder builder = new StringBuilder();
		for (LTCFragment frag : fragments) {
			builder.append(frag.startKey);
			builder.append(",");
			builder.append(frag.endKey);
			builder.append(",");
			builder.append(frag.ltcServerId);
			builder.append(",");
			builder.append(frag.dbId);
			builder.append(",");
			for (int i = 0; i < frag.stocLogReplicaServerIds.size(); i++) {
				builder.append(frag.stocLogReplicaServerIds.get(i));
				builder.append(",");
			}
			builder.deleteCharAt(builder.length() - 1);
			builder.append("\n");
		}
		bw.write(builder.toString());
		bw.flush();
		bw.close();
	}

	public static void main(String[] args) throws Exception {
		int index = 0;
		String outputPath = args[index++];
		String arch = args[index++];
		if ("shared".equals(arch)) {
			int nRecords = Integer.parseInt(args[index++]);
			int nCCServers = Integer.parseInt(args[index++]);
			int nLogReplicasPerRange = Integer.parseInt(args[index++]);
			int nRangesPerServer = Integer.parseInt(args[index++]);
			generateSharedCCRangePartitionConfig(outputPath, nRecords,
					nCCServers, nLogReplicasPerRange, nRangesPerServer);
		} else {
			String comp = args[index++];
			if ("cc".equals(comp)) {
				int nRecords = Integer.parseInt(args[index++]);
				int nCCServers = Integer.parseInt(args[index++]);
				int nDCServers = Integer.parseInt(args[index++]);
				int nLogReplicasPerRange = Integer.parseInt(args[index++]);
				int nRangesPerServer = Integer.parseInt(args[index++]);
				generateCCRangePartitionConfig(outputPath, nRecords, nCCServers,
						nLogReplicasPerRange, nRangesPerServer, nDCServers);
			}
		}
	}
}
