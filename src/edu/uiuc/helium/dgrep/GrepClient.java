package edu.uiuc.helium.dgrep;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.helium.proto.MessageProtos.GrepMessage;

/**
 * The class acts like a distributed Grep to user. It a) receives Grep pattern
 * from user b) Finds all the Live Nodes that are part of Distributed System c)
 * Sends grep request to all the live nodes d) Collates the Replies from all
 * nodes and presents to user
 */
public class GrepClient {
	private static final Logger logger = LoggerFactory
			.getLogger(GrepClient.class);

	private int grepServerPort;
	private ExecutorService workers;

	private List<String> allHosts;

	/**
	 * 
	 * @param grepServerPort
	 *            The port where GrepServer on each host is running
	 * @param poolsize
	 *            The number of nodes the GrepClient can communicate in parallel
	 * @param failureMonitorPort
	 *            The port where FailureMonitor on each host is running
	 * @param allHosts
	 */
	public GrepClient(int grepServerPort, int poolsize, int failureMonitorPort,
			List<String> allHosts) {
		this.grepServerPort = grepServerPort;
		this.workers = Executors.newFixedThreadPool(poolsize);
		this.allHosts = allHosts;
	}

	/**
	 * 
	 * @param args
	 *            Command of the form [ --key, <<pattern>> ] or [ --value,
	 *            <<pattern>> ]
	 * @throws InterruptedException
	 */
	private void grep(String[] args) throws InterruptedException {
		long before = System.currentTimeMillis();
		/**
		 * Get the list of Live nodes that are currently part of Distributed
		 * System
		 */
		List<GrepRequest> tasks = new ArrayList<GrepRequest>();
		/**
		 * Create a GrepRequest corresponding to each Live Host. Each host will
		 * process this GrepRequest and send back the results
		 */
		for (String host : allHosts) {
			tasks.add(new GrepRequest(host, grepServerPort, args));
		}
		/**
		 * wait for all the hosts to finish sending the reply
		 */
		List<Future<GrepMessage>> results = workers.invokeAll(tasks);
		long after = System.currentTimeMillis();
		/**
		 * Now that all the hosts have sent the GrepReply, present results to
		 * user.
		 */
		for (Future<GrepMessage> result : results) {
			try {
				GrepMessage grepReply = result.get();
				if (grepReply != null)
					printGrepReplyMessage(grepReply);
			} catch (ExecutionException e) {
				logger.error("Error while waiting", e);
			}
		}
		System.out.println("");
		System.out.format("Grepped %d hosts in %d milliseconds\n",
				allHosts.size(), after - before);

	}

	/**
	 * Nicely formats and prints the grep output to console
	 * 
	 * @param msg
	 *            reply message containing output for grep command
	 */
	private void printGrepReplyMessage(GrepMessage msg) {
		System.out.println(String.format(
				"Host: %s Matched Lines Count: %d LogFile: %s",
				msg.getSenderAddress(), msg.getMatchedLinesCount(),
				msg.getLogFileName()));
		for (String matchedLine : msg.getMatchedLinesList())
			System.out.println(matchedLine);
		System.out.println("-------------------------------------------------");
		System.out.println("");
	}

	public static void main(String[] args) throws Exception {
		/*
		 * checks that command syntax is valid
		 */
		boolean isValid = Grep.isValidCommand(args);
		if (!isValid) {
			System.out
					.println("Command syntax is invalid. Syntax is grep --key <<pattern>> \n grep --value <<pattern>>");
			return;
		}
		/**
		 * Read required properties from the config file
		 */
		String configFilePath = System.getProperty("app.config");
		GrepConfig config = new GrepConfig(configFilePath);
		try {
			config.init();
		} catch (Exception e) {
			logger.info("Error initializing GrepConfig", e);
		}

		/**
		 * Initialize the GrepClient and query for the command input by user
		 */
		GrepClient client = new GrepClient(config.getGrepServerPort(),
				config.getThreadPoolSize(), config.getFailureMonitorPort(),
				config.getAllHosts());
		client.grep(args);
		System.exit(0);
	}
}
