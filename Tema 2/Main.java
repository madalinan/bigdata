package com.logs.process;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

	private static Map<String, Long> aggregateBytes(
			Map<Integer, Long> firstMap, Map<Integer, Long> secondMap) {
		for (Entry<Integer, Long> entry : secondMap.entrySet()) {
			Integer key = entry.getKey();
			if (!firstMap.containsKey(key)) {
				firstMap.put(key, (long) 0);
			}
			firstMap.put(key, firstMap.get(key) + secondMap.get(key));
		}
		return null;
	}

	public static void main(String[] args) {
		Map<Integer, Long> ipBytesMap = new HashMap<Integer, Long>();
		Date startDate = new Date();

		String logsPath = "logs";
		String ip = "178.154.179.250";// "66.249.74.147";

		FilenameFilter filesFilter = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains(".log.zip");
			}
		};
		List<File> logsList = Arrays.asList(new File(logsPath)
				.listFiles(filesFilter));

		int THREADS_NO = 5;
		ExecutorService executorService = Executors
				.newFixedThreadPool(THREADS_NO);
		CompletionService<LogProcessor> completionService = new ExecutorCompletionService<LogProcessor>(
				executorService);

		for (int process = 0; process < logsList.size(); process++) {
			completionService
					.submit(new LogProcessor(logsList.get(process), ip));
		}

		for (int process = 0; process < logsList.size(); process++) {
			try {
				aggregateBytes(ipBytesMap, completionService.take().get()
						.getIpBytesMap());
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new IllegalStateException(
						"Got interrupt exception when processing log data.", e);
			} catch (ExecutionException e) {
				e.printStackTrace();
				throw new IllegalStateException(
						"Got execution exception when processing log data.", e);
			}
		}
		executorService.shutdown();

		long totalBytes = 0;
		for (Entry<Integer, Long> entry : ipBytesMap.entrySet()) {
			totalBytes += entry.getValue();
		}
		System.out.println("IP bytes: " + ipBytesMap);
		System.out.println("total bytes: " + totalBytes);

		for (Entry<Integer, Long> entry : ipBytesMap.entrySet()) {
			long entryBytes = entry.getValue();
			float percent = (entryBytes * 100.0f) / totalBytes;
			System.out.println("status = " + entry.getKey() + " - bytes = "
					+ entryBytes + " - percent = " + percent);
		}

		Date endDate = new Date();
		System.out.println("Elapsed time = "
				+ (endDate.getTime() - startDate.getTime()) + "ms");
	}

}
