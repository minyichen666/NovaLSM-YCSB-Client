package com.yahoo.ycsb.db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ConfigurationUtil {
	public static class Configuration {
		List<List<LTCFragment>> configs = Lists.newArrayList();
		AtomicInteger configurationId = new AtomicInteger(0);
		
		public List<LTCFragment> current() {
			return configs.get(configurationId.intValue());
		}
	}

	public static Configuration readConfig(String configPath) {
		Configuration config = new Configuration();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(configPath)));
			String line = null;
			List<LTCFragment> fragments = Lists.newArrayList();
			while ((line = br.readLine()) != null) {
				if (line.contains("config")) {
					if (!fragments.isEmpty()) {
						config.configs.add(fragments);
					}
					fragments = Lists.newArrayList();
				}

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
			config.configs.add(fragments);
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return config;
	}

	public static List<LTCFragment> generateConfig(String outputPath, int nRecords, int nLTCServers,
			int nLogReplicasPerRange, int nRangesPerServer) throws Exception {
		int tRanges = nRangesPerServer * nLTCServers;
		int nRecordsPerRange = nRecords / tRanges;
		List<LTCFragment> fragments = Lists.newArrayList();
		int startKey = 0;
		int dbId = 0;

		for (int i = 0; i < nLTCServers; i++) {
			for (int j = 0; j < nRangesPerServer; j++) {
				LTCFragment frag = new LTCFragment();
				frag.startKey = startKey;
				frag.endKey = startKey + nRecordsPerRange;
				frag.ltcServerId = i;
				frag.dbId = dbId;
				dbId++;
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
		return fragments;
	}

	private static void writeConfig(String outputPath, int nRecords, int nLTCServers, int nLogReplicasPerRange,
			int nRangesPerServer, List<List<LTCFragment>> configs) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(
				new File(String.format("%s/nova-shared-cc-nrecords-%d-nccservers-%d-nlogreplicas-%d-nranges-%d",
						outputPath, nRecords, nLTCServers, nLogReplicasPerRange, nRangesPerServer))));
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < configs.size(); i++) {
			builder.append("config-" + i);
			builder.append("\n");
			for (LTCFragment frag : configs.get(i)) {
				builder.append(frag.toConfigString());
				builder.append("\n");
			}
		}

		bw.write(builder.toString());
		bw.flush();
		bw.close();
	}

	private static List<LTCFragment> constructRanges(TreeMap<Integer, Double> sortedMap, double totalShares, int lower,
			int upper, int numberOfFragments) {
		assert upper - lower > 1;
		assert numberOfFragments > 1;
		List<LTCFragment> tmp_subranges = Lists.newArrayList();
		double sharePerSubRange = totalShares / numberOfFragments;
		double total = totalShares;
		double currentRate = 0;

		LTCFragment currentRange = new LTCFragment();
		currentRange.startKey = lower;
		for (Entry<Integer, Double> entry : sortedMap.entrySet()) {
			assert entry.getKey() >= lower;
			assert entry.getKey() < upper;

			double rate = entry.getValue();
			if (currentRate + rate > sharePerSubRange) {
				if (currentRange.startKey == entry.getKey()) {
					currentRate += rate;
					currentRange.refCount = currentRate;
					currentRange.endKey = entry.getKey() + 1;
					tmp_subranges.add(currentRange);
					currentRange = new LTCFragment();

					currentRange.startKey = entry.getKey() + 1;
					if (tmp_subranges.size() + 1 == numberOfFragments) {
						break;
					}
					currentRate = 0;
					totalShares -= rate;
					sharePerSubRange = totalShares / (numberOfFragments - tmp_subranges.size());
					continue;
				} else {
					currentRange.refCount = currentRate;
					currentRange.endKey = entry.getKey();

					tmp_subranges.add(currentRange);
					currentRange = new LTCFragment();

					currentRange.startKey = entry.getKey();
					if (tmp_subranges.size() + 1 == numberOfFragments) {
						break;
					}
					currentRate = 0;
					sharePerSubRange = totalShares / (numberOfFragments - tmp_subranges.size());
				}
			}
			currentRate += rate;
			totalShares -= rate;
		}
		if (currentRange.startKey < upper) {
			currentRange.refCount = currentRate;
			tmp_subranges.add(currentRange);
		}
		assert tmp_subranges.size() <= numberOfFragments;

		tmp_subranges.get(0).startKey = lower;
		tmp_subranges.get(tmp_subranges.size() - 1).endKey = upper;
		return tmp_subranges;
	}

	static List<Double> refs = Lists.newArrayList();

	public static void readZipfDist(int nRecords, double zipf) throws Exception {
		BufferedReader br = new BufferedReader(
				new FileReader(new File(String.format("/tmp/zipfian-%d-%.2f", nRecords, zipf))));
		String line = null;
		while ((line = br.readLine()) != null) {
			double refcount = Double.parseDouble(line);
			refs.add(refcount);
		}
		br.close();
	}

	public static List<LTCFragment> updateConfigurationBasedOnLoad(int nRecords, int numberOfLTCs,
			List<LTCFragment> fragments) throws Exception {
		Map<Integer, Double> serverLoad = Maps.newHashMap();
		List<LTCFragment> balanceLoadFragments = Lists.newArrayList();
		for (LTCFragment frag : fragments) {
			balanceLoadFragments.add(frag.copy());
		}
		double sum = 0;
		for (LTCFragment frag : balanceLoadFragments) {
			serverLoad.compute(frag.ltcServerId, (k, v) -> {
				if (v == null) {
					return frag.refCount;
				}
				return v + frag.refCount;
			});
			sum += frag.refCount;
		}

		for (int ltcId = 0; ltcId < numberOfLTCs; ltcId++) {
			System.out.println("LTC " + ltcId + " " + serverLoad.get(ltcId) / sum);
		}

		double loadPerLTC = sum / numberOfLTCs;
		Set<Integer> movedFrags = Sets.newHashSet();
		int ltcId = 0;
		// Move fragment to other LTCs.
		for (int fragId = 0; fragId < balanceLoadFragments.size(); fragId++) {
			LTCFragment frag = balanceLoadFragments.get(fragId);
			if (frag.ltcServerId != ltcId) {
				continue;
			}

			if (!movedFrags.contains(fragId)) {
				for (int otherLTCId = ltcId + 1; otherLTCId < numberOfLTCs; otherLTCId++) {
					if (serverLoad.get(otherLTCId) > loadPerLTC) {
						continue;
					}
					System.out.println("Migrate fragment " + frag.toString() + " " + otherLTCId);
					serverLoad.compute(frag.ltcServerId, (k, v) -> {
						return v - frag.refCount;
					});
					serverLoad.compute(otherLTCId, (k, v) -> {
						return v + frag.refCount;
					});
					frag.ltcServerId = otherLTCId;
					movedFrags.add(fragId);
					break;
				}
				if (serverLoad.get(ltcId) < loadPerLTC) {
					break;
				}
			}
		}
		for (ltcId = 0; ltcId < numberOfLTCs; ltcId++) {
			System.out.println("LTC " + ltcId + " " + serverLoad.get(ltcId) / sum);
		}
		System.out.println("Migrated " + movedFrags.size() + " Total " + fragments.size());
		return balanceLoadFragments;
	}

	public static void main(String[] args) throws Exception {
		int index = 0;
		String outputPath = args[index++];
		String arch = args[index++];
		int nRecords = Integer.parseInt(args[index++]);
		int nLTCServers = Integer.parseInt(args[index++]);
		int nLogReplicasPerRange = Integer.parseInt(args[index++]);
		int nRangesPerServer = Integer.parseInt(args[index++]);

		if ("shared".equals(arch)) {
			List<LTCFragment> fragments = generateConfig(outputPath, nRecords, nLTCServers, nLogReplicasPerRange,
					nRangesPerServer);
			List<List<LTCFragment>> configurations = Lists.newArrayList();
			configurations.add(fragments);
			writeConfig(outputPath, nRecords, nLTCServers, nLogReplicasPerRange, nRangesPerServer, configurations);
		} else if ("migration".equals(arch)) {
			double zipf = Double.parseDouble(args[index++]);
			readZipfDist(nRecords, zipf);

			List<List<LTCFragment>> configurations = Lists.newArrayList();
			List<LTCFragment> configuration = Lists.newArrayList();
			int nRecordsPerLTC = nRecords / nLTCServers;
			int dbid = 0;
			for (int ltcId = 0; ltcId < nLTCServers; ltcId++) {
				TreeMap<Integer, Double> sortedMap = Maps.newTreeMap();
				double totalShares = 0;
				int lower = ltcId * nRecordsPerLTC;
				int upper = (ltcId + 1) * nRecordsPerLTC;
				if (ltcId == nLTCServers - 1) {
					upper = nRecords;
				}
				for (int recordId = lower; recordId < upper; recordId++) {
					sortedMap.put(recordId, refs.get(recordId));
					totalShares += refs.get(recordId);
				}
				System.out
						.println("Create fragment for server " + ltcId + " " + totalShares + " " + lower + " " + upper);
				List<LTCFragment> fragments = constructRanges(sortedMap, totalShares, lower, upper, nRangesPerServer);
				for (LTCFragment frag : fragments) {
					frag.dbId = dbid++;
					frag.ltcServerId = ltcId;
				}
				configuration.addAll(fragments);
			}
			List<LTCFragment> balanced = updateConfigurationBasedOnLoad(nRecords, nLTCServers, configuration);
			configurations.add(configuration);
			configurations.add(balanced);
			writeConfig(outputPath, nRecords, nLTCServers, nLogReplicasPerRange, nRangesPerServer, configurations);
		}
	}
}
