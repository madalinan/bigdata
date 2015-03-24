package com.logs.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;

public class LogProcessor implements Callable<LogProcessor> {
	private static final String LOG_ENTRY_PATTERN = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
	public static final int NUM_FIELDS = 9;

	private File logFile;
	private String ip;
	private long bytes = 0;

	private Map<String, Long> ipBytesMap = new HashMap<String, Long>();

	public LogProcessor(File logFile, String ip) {
		this.logFile = logFile;
		this.ip = ip;
	}

	public long getBytes() {
		return bytes;
	}

	public Map<String, Long> getIpBytesMap() {
		return ipBytesMap;
	}

	private void processLog() throws IOException {
		String line = null;
		BufferedReader br = null;
		try {
			GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(logFile));
			br = new BufferedReader(new InputStreamReader(gzipInputStream));
			while ((line = br.readLine()) != null) {
				Pattern p = Pattern.compile(LOG_ENTRY_PATTERN);
				Matcher matcher = p.matcher(line);
				if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
					System.out.println("Warn: Ignoring bad log entry in " + logFile + ":\n" + line);
				} else {
					String parsedIP = matcher.group(1);
					long ipBytes = Long.valueOf(matcher.group(7));

					if (!ipBytesMap.containsKey(parsedIP)) {
						ipBytesMap.put(parsedIP, (long) 0);
					}
					ipBytesMap.put(parsedIP, ipBytesMap.get(parsedIP) + ipBytes);

//					 if (parsedIP.equals(ip)) {
//					 bytes += ipBytes;
//					 }
				}
			}
		} catch (IOException e) {
			throw new IOException("Exception when processing log file: " + logFile.getAbsolutePath(), e);
		} finally {
			IOUtils.closeQuietly(br);
		}
	}

	@Override
	public LogProcessor call() throws Exception {
		processLog();
		System.out.println("Processed log file: " + logFile);
		return this;
	}
}
