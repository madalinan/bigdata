package com.logs.process;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
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

import com.logs.process.ProcessLogs;

public class ProcessLogs {

	private static Map<String, Long> aggregateBytes(Map<String, Long> firstMap, Map<String, Long> secondMap) {
		for (Entry<String, Long> entry : secondMap.entrySet()) {
			String key = entry.getKey();
			if (!firstMap.containsKey(key)) {
				firstMap.put(key, (long) 0);
			}
			firstMap.put(key, firstMap.get(key) + secondMap.get(key));
		}
		return null;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Map<String, Long> ipBytesMap = new HashMap<String, Long>();
		Date startDate = new Date();

		String logsPath = "logs";
		String ip = "213.239.211.141";// IP bytes: 153498

		FilenameFilter filesFilter = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains(".log.zip");
			}
		};
		List<File> logsList = Arrays.asList(new File(logsPath).listFiles(filesFilter));

		long bytes = 0;
		int THREADS_NO = 10;
		ExecutorService executorService = Executors.newFixedThreadPool(THREADS_NO);
		CompletionService<LogProcessor> completionService = new ExecutorCompletionService<LogProcessor>(executorService);

		for (int process = 0; process < logsList.size(); process++) {
			completionService.submit(new LogProcessor(logsList.get(process), ip));
		}

		for (int process = 0; process < logsList.size(); process++) {
			try {
				aggregateBytes(ipBytesMap, completionService.take().get().getIpBytesMap());
//				bytes += completionService.take().get().getBytes();
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new IllegalStateException("Got interrupt exception when processing log data.", e);
			} catch (ExecutionException e) {
				e.printStackTrace();
				throw new IllegalStateException("Got execution exception when processing log data.", e);
			}
		}
		executorService.shutdown();

		Date endDate = new Date();
//		System.out.println("IP bytes: " + bytes);

		System.out.println("IP bytes:\n" + ipBytesMap);
		System.out.println("Elapsed time = " + (endDate.getTime() - startDate.getTime()) + "ms");
	}
}
