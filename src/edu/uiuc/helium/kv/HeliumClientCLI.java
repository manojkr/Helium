package edu.uiuc.helium.kv;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.AllPermission;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import edu.uiuc.helium.gossip.FailureMonitor;
import edu.uiuc.helium.gossip.HeliumConfig;
import edu.uiuc.helium.proto.MessageProtos.KVMessage.ConsistencyLevel;

/**
 * 
 * Command Line Tool to access Key Value Store. It provides a set of commands to
 * access Key Value store via command line
 * 
 * @author Manoj
 * 
 */
public class HeliumClientCLI {

	private static final Logger logger = LoggerFactory
			.getLogger(HeliumClientCLI.class);

	private HeliumClient kvclient;
	private HeliumConfig config;

	public HeliumClientCLI(HeliumConfig config) {
		this.config = config;
	}

	public void init() {
		kvclient = new HeliumClient(config);
		kvclient.init();
	}

	@Command
	public void show() {
		System.out.println(kvclient.show());
	}

	/**
	 * Adds a (key,value) to the Key Value Store
	 * 
	 * @param key
	 *            Integer key
	 * @param value
	 *            Value object corresponding to the Integer key
	 */
	@Command
	public void insert(String key, String value, String consistencyLevel) {
		try {

			long before = System.currentTimeMillis();
			ConsistencyLevel level = ConsistencyLevel.valueOf(consistencyLevel
					.trim().toUpperCase());
			kvclient.put(key, new HeliumValue(value, new Date().getTime()),
					level);
			long after = System.currentTimeMillis();
			System.out
					.format("SUCCESS PUT KEY: %s VALUE: %s Consistency Level: %s to KVStore in %d ms\n",
							key, value, level, (after - before));
		} catch (HeliumException e) {
			System.out.println(String.format(
					"FAILURE PUT KEY: %s VALUE: %s to KVStore", key, value));
			System.out.println(e.getMessage());
		}
	}

	/**
	 * Retrieves the (key,value) pair corresponding to given key
	 * 
	 * @param key
	 *            Integer Key
	 */
	@Command
	public void lookup(String key, String consistencyLevel) {
		try {
			long before = System.currentTimeMillis();
			ConsistencyLevel level = ConsistencyLevel.valueOf(consistencyLevel
					.trim().toUpperCase());
			String value = (String) kvclient.get(key, level).getValue();
			long after = System.currentTimeMillis();
			System.out
					.format("SUCCESS LOOKUP KEY: %s \n VALUE: %s \nConsistency Level: %s in %d ms\n",
							key, value, level, (after - before));
		} catch (HeliumException e) {
			System.out.println(String.format("FAILURE LOOKUP KEY: %s", key));
			System.out.println(e.getMessage());
		}
	}

	/**
	 * Updates the value for a key in the Key Value store
	 * 
	 * @param key
	 *            Integer Key
	 * @param value
	 *            New Value for the key
	 */
	@Command
	public void update(String key, String value, String consistencyLevel) {
		try {
			long before = System.currentTimeMillis();
			ConsistencyLevel level = ConsistencyLevel.valueOf(consistencyLevel
					.trim().toUpperCase());
			String oldValue = (String) kvclient.update(key,
					new HeliumValue(value, new Date().getTime()), level)
					.getValue();
			long after = System.currentTimeMillis();
			System.out
					.format("SUCCESS UPDATE KEY: %s OLD_VALUE: %s NEW_VALUE: %s Consistency Level: %s in %d ms\n",
							key, oldValue, value, level, (after - before));
		} catch (HeliumException e) {
			System.out.println(String.format(
					"FAILURE update KEY: %s Value: %s", key, value));
			System.out.println(e.getMessage());
		}
	}

	/**
	 * Removes the entry for key from the Key Value store
	 * 
	 * @param key
	 */
	@Command
	public void delete(String key, String consistencyLevel) {
		try {
			long before = System.currentTimeMillis();
			ConsistencyLevel level = ConsistencyLevel.valueOf(consistencyLevel
					.trim().toUpperCase());
			String value = (String) kvclient.remove(key, level).getValue();
			long after = System.currentTimeMillis();
			System.out
					.format("SUCCESS DELETE KEY: %s VALUE: %s Consistency Level: %s in %d ms\n",
							key, value, level, (after - before));
		} catch (HeliumException e) {
			System.out.println(String.format("FAILURE update KEY: %s", key));
			System.out.println(e.getMessage());
		}
	}


	/**
	 * Required for cool application, 
	 * get a Set containing stop words.  
	 */
	private Set<String> getStopWords() {
		Set<String> stopWords = new HashSet<String>();
		String file = System.getProperty("stop_word_list");
		String words;
		try {
			BufferedReader r = new BufferedReader(new FileReader(file));
			words = r.readLine();
			r.close();
			String[] split = words.split(",");
			for (String w : split) {
				stopWords.add(w);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error reading stop words");
		}
		return stopWords;
	}

	/**
	 * Load Google Scholar DAta Set in the key 
	 * value store
	 */
	@Command
	public void loadGoogleScholarDataset() {
		Map<String, String> keywords = new HashMap<String, String>();
		Set<String> stopWords = getStopWords();
		String dataset = System.getProperty("scholar_dataset_file");
		int count = Integer.MAX_VALUE;
		String readCount = System.getProperty("scholar_count");
		if (readCount != null)
			count = Integer.parseInt(readCount);
		System.out.format("Reading 1000 records from dataset: %s \n", 
				dataset);
		int counter = 0;
		try {
			BufferedReader r = new BufferedReader(new FileReader(dataset));
			String nextLine = null;
			while ((nextLine = r.readLine()) != null) {
				counter++;
				if (counter > count)
					break;
				String[] parts = nextLine.split(",");
				if (parts.length < 2)
					continue;
				String title = parts[1].substring(1, parts[1].length() - 1)
						.trim().toLowerCase();
				String[] words = title.split("\\s+");
				for (String word : words) {
					if (keywords.containsKey(word)) {
						String oldVal = keywords.get(word);
						String newVal = oldVal + "\n" + title;
						keywords.put(word, newVal);
					} else {
						keywords.put(word, title);
					}
				}
			}
		} catch (Exception e) {
			System.out.println("Error reading dataset");
			e.printStackTrace();
		}
		System.out.println("Loading data into Helenus now");
		for (String word : keywords.keySet()) {
			try {
				if (!stopWords.contains(word)) {
					kvclient.put(word, new HeliumValue(keywords.get(word),
							new Date().getTime()), ConsistencyLevel.ALL);
				}
			} catch (HeliumException e) {
				e.printStackTrace();
				System.out.format("Error putting key: %s\n", word);
			}
		}

	}

	@Command
	public void exit() {
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		String configFile = System.getProperty("app.config");
		/**
		 * Read the config file to extract the KeyValue store specific
		 * parameters from config file
		 */
		HeliumConfig config = new HeliumConfig(configFile);
		try {
			config.init();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("ERROR: Could not initialize KVClientCLI due to"
					+ e.getMessage());
		}

		/**
		 * Initialize the Key Value Command Line tool
		 */
		HeliumClientCLI cli = new HeliumClientCLI(config);
		cli.init();
		/*
		 * TimeUnit.SECONDS.sleep(10); cli.lookup(5, "ALL");
		 */
		ShellFactory.createConsoleShell("CS425", "", cli).commandLoop();
	}
}
