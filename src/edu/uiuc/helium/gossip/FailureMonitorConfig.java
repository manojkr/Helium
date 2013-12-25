package edu.uiuc.helium.gossip;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * GrepConfig class a) reads the configuration file b) and extracts the required
 * config parameters which FailureMonitor and GossipManager requires
 */
public class FailureMonitorConfig {
	private static final Logger logger = LoggerFactory
			.getLogger(FailureMonitorConfig.class);

	private File configFile;
	private int listenPort;
	private int kvServerListenPort;
	private int nodeCount;
	private List<String> hosts;
	private long gossipInterval;
	private long tFail;
	private long tCleanup;
	private double messageLossRate;

	private int kvServerCircleSize;

	public FailureMonitorConfig(File configFile) {
		this.configFile = configFile;
	}

	public FailureMonitorConfig(String configFile) {
		this.configFile = new File(configFile);
	}

	public void init() throws Exception {
		logger.info("Initializing FailureMonitorConfig from file: {}",
				configFile);

		Configuration config = new PropertiesConfiguration(configFile);
		listenPort = config.getInt("failure_monitor_port");
		nodeCount = config.getInt("node_count");
		messageLossRate = config.getDouble("message_loss_rate");

		hosts = new ArrayList<String>();
		for (int i = 1; i <= nodeCount; i++) {
			String hostName = config.getString(String.format("node.%d.ip", i));
			hosts.add(hostName);
		}

		gossipInterval = config.getLong("gossip_interval");
		tFail = config.getLong("tfail");
		tCleanup = config.getLong("tcleanup");
		kvServerListenPort = config.getInt("kvserver_listen_port");
		kvServerCircleSize = config.getInt("kvserver_circle_size");
		System.out.println(hosts);

	}

	public int getListenPort() {
		return listenPort;
	}

	/**
	 * @return The number of systems that are configured to be part of
	 *         Distributed System
	 */
	public int getNodeCount() {
		return nodeCount;
	}

	/**
	 * 
	 * @return Ip Addresses of systems that are configured to be part of
	 *         Distribute System
	 */
	public List<String> getHosts() {
		return hosts;
	}

	/**
	 * 
	 * @return TimeInterval in milliseconds at which Gossip Messages will be
	 *         sent
	 */
	public long getGossipInterval() {
		return gossipInterval;
	}

	/**
	 * 
	 * @return Time in milliseconds after which a host is assumed to be fail if
	 *         no gossip is received for that host
	 */
	public long gettFail() {
		return tFail;
	}

	/**
	 * 
	 * @return Time in milliseconds to wait for removing a host entry after the
	 *         host has been assumed to have failed
	 */
	public long gettCleanup() {
		return tCleanup;
	}

	/**
	 * 
	 * @return Message Loss Rate in percentage. Out of 100 gossip messages, this
	 *         number of gossip messages will be dropped
	 */
	public double getMessageLossRate() {
		return messageLossRate;
	}

	public int getKVServerListenPort() {
		return kvServerListenPort;
	}
	
	public int getKVServerCircleSize() {
		return kvServerCircleSize;
	}

}
