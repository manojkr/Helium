package edu.uiuc.helium.kv;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import edu.uiuc.helium.gossip.HeliumConfig;

/**
 * Command line client for KVServer. It provides commands to see all the local
 * keys on the server and all the live KVServers
 * 
 * @author Manoj
 * 
 */
public class HeliumServerCLI {

	private static final Logger logger = LoggerFactory
			.getLogger(HeliumServerCLI.class);
	private HeliumConfig config;
	private HeliumServer kv;

	public HeliumServerCLI(HeliumConfig config) {
		this.config = config;
	}

	/**
	 * Start the KVServer
	 */
	public void init() {
		kv = new HeliumServer(config);
		kv.init();
		logger.info("Starting new thread for KVServer");
		Thread kvServerThread = new Thread(kv);
		kvServerThread.start();
		logger.info("Exiting now");
	}

	/**
	 * Display all the Key/Value pairs on the KVServer and all the live
	 * KVServers
	 */
	@Command
	public void show() {
		System.out.println("Key/Value pairs on this server are");
		System.out.println(kv.showData());
		System.out.println("Following KVServers are part of the group");
		System.out.println(kv.showAllKVServers());
		System.out.println("Replica group of this nodes consist of "
				+ kv.showReplicaGroup());
	}

	@Command
	public void history() {
		System.out.println("Last 10 read commands are");
		System.out.println(kv.getReadCommands());
		System.out.println("Last 10 write commands are");
		System.out.print(kv.getWriteCommands());
	}

	/**
	 * Command to leave the Group
	 */
	@Command
	public void leave() {
		System.out.println("Leaving the group of KVServers");
		kv.leave();
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		String configFileName = System.getProperty("app.config");
		HeliumConfig config = new HeliumConfig(configFileName);
		try {
			config.init();
		} catch (Exception e) {
			System.out.println("ERROR: Could not initialize KVServerCLI due to"
					+ e.getMessage());
			System.exit(-1);
		}

		/**
		 * Having read the config file,Start the Grep Server Thead now
		 */
		HeliumServerCLI cli = new HeliumServerCLI(config);
		cli.init();
		ShellFactory.createConsoleShell("CS425", "", cli).commandLoop();
	}

}
