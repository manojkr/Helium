package edu.uiuc.helium.dgrep;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * GrepConfig class a) reads the configuration file b) and extracts the required
 * config parameters which GrepClient and GrepServer requires
 */
public class GrepConfig {

	private static final Logger logger = LoggerFactory
			.getLogger(GrepConfig.class);

	private File configFile;
	/**
	 * Port Number where GrepServer is running on each host and accepting the
	 * GrepRequests
	 */
	private int grepServerPort;
	/**
	 * Port Number where FailureMonitor is running on each host and maintaining
	 * the list of live nodes in network
	 */
	private int failureMonitorPort;
	/**
	 * Number of concurrent requests which the GrepServer can process in
	 * parallel
	 */
	private int threadPoolSize;
	/**
	 * Number of nodes that are expected to be part of Distributed System
	 */
	private int nodeCount;

	/**
	 * All nodes that can be part of network
	 */
	private List<String> allNodes = new ArrayList<String>();

	/**
	 * log file location for each node that is part of System
	 */
	private Map<String, String> logFileLocations = new HashMap<String, String>();

	public GrepConfig(File configFile) {
		this.configFile = configFile;
	}

	public GrepConfig(String configFile) {
		this.configFile = new File(configFile);
	}

	/**
	 * Reads the Config File and initializes the required parameters
	 * 
	 * @throws Exception
	 *             Throws exception if there is some error during initialization
	 */
	public void init() throws Exception {
		// logger.info("Initializing configuration from file: {}", configFile);

		Configuration config = new PropertiesConfiguration(configFile);
		grepServerPort = config.getInt("grep_server_port");
		threadPoolSize = config.getInt("thread_pool_size");
		nodeCount = config.getInt("node_count");

		for (int i = 1; i <= nodeCount; i++) {
			String nodeIp = config.getString(String.format("node.%d.ip", i));
			allNodes.add(nodeIp);
			String logFilePath = config.getString(String.format(
					"node.%d.logfilepath", i));
			logFileLocations.put(nodeIp, logFilePath);
		}

	}

	/**
	 * 
	 * @return Port Number where GrepServer is running on each host and
	 *         accepting the GrepRequests
	 */
	public int getGrepServerPort() {
		return grepServerPort;
	}

	/**
	 * 
	 * @return Port Number where FailureMonitor is running on each host and
	 *         maintaining the list of live nodes in network
	 */
	public int getFailureMonitorPort() {
		return failureMonitorPort;
	}

	/**
	 * 
	 * @return Number of concurrent requests which the GrepServer can process in
	 *         parallel
	 */
	public int getThreadPoolSize() {
		return threadPoolSize;
	}

	/**
	 * @param hostname
	 * @return log file location for hostname
	 */
	public String getLogFilePath(String hostname) {
		return logFileLocations.get(hostname);
	}

	public List<String> getAllHosts() {
		return allNodes;
	}

}
