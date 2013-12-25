package edu.uiuc.helium.kv;

import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.uiuc.helium.gossip.FailureMonitor;
import edu.uiuc.helium.gossip.HeliumConfig;
import edu.uiuc.helium.gossip.MembershipChangeListener;
import edu.uiuc.helium.proto.MessageProtos.KVMessage;
import edu.uiuc.helium.proto.MessageProtos.KVMessage.ConsistencyLevel;
import edu.uiuc.helium.proto.MessageProtos.KVMessage.MessageType;

/**
 * 
 * API to access Key Value Store. It provides a set of methods to access Key
 * Value store programmatically
 * 
 * @author Manoj
 */
public class HeliumClient implements MembershipChangeListener {
	private static final Logger logger = LoggerFactory
			.getLogger(HeliumClient.class);
	private HeliumConfig config;
	private ConsistentHash consistentHash;
	private FailureMonitor fm;
	private int kvServerListenPort;
	private int circleLength;

	public HeliumClient(HeliumConfig config) {
		this.config = config;
		this.circleLength = config.getKVServerCircleSize();
		this.kvServerListenPort = config.getKVServerListenPort();
		this.consistentHash = new ConsistentHash(circleLength);
	}

	public String show() {
		StringBuilder out = new StringBuilder();
		out.append(consistentHash.showNodePositions());
		out.append("\n");
		out.append(fm.getActiveLiveHosts());
		return out.toString();
	}

	public void init() {
		/**
		 * Initialize the FailureMonitor thread. This is required for Membership
		 * detection to know which KVServers are part of group
		 */
		fm = new FailureMonitor(config, false);
		fm.registerMembershipChangeListener(this);
		Thread fmThread = new Thread(fm);
		fmThread.start();
	}

	/**
	 * Retrieves the (key,value) pair corresponding to given key
	 * 
	 * @param key
	 *            Integer Key
	 * @return Value object corresponding to given key
	 * @throws HeliumException
	 */
	public HeliumValue get(String key, ConsistencyLevel consistencyLevel)
			throws HeliumException {
		long before = System.currentTimeMillis();
		/**
		 * Find out which KVServer is storing this key
		 */
		String toHost = consistentHash.getNodeForKey(key);
		if (toHost == null)
			throw new HeliumException("No KVServer is available");
		/**
		 * Create a GET_REQUEST for the key to the KVServer
		 */
		KVMessage kvmessage = KVMessage.newBuilder()
				.setType(MessageType.GET_REQUEST).setKey(key)
				.setConsistencyLevel(consistencyLevel).build();
		logger.info("Sending GET request to {} for Key: {}", toHost, key);
		KVMessage reply = HeliumSocketUtils.sendAndReceiveKVMessage(kvmessage,
				toHost, kvServerListenPort);
		long after = System.currentTimeMillis();
		logger.info("LATENCY GET KEY: {} in {} ms Level: {}", key,
				(after - before), consistencyLevel);
		/**
		 * Check if the operation was successful
		 */
		if (reply == null)
			throw new HeliumException("Error in performing GET operation");
		if (reply.getType() == MessageType.REPLY_FAILURE_KEY_ERROR)
			throw new HeliumException("Key is not present in any KVServer");
		HeliumValue value = (HeliumValue) SerializationUtils
				.deserialize(reply.getValue().toByteArray());
		return value;

	}

	/**
	 * Adds a (key,value) to the Key Value Store
	 * 
	 * @param key
	 *            Integer key
	 * @param value
	 *            Value object corresponding to the Integer key
	 * @throws HeliumException
	 */
	public void put(String key, HeliumValue value,
			ConsistencyLevel consistencyLevel) throws HeliumException {
		long before = System.currentTimeMillis();
		/**
		 * Find out which KVServer is storing this key
		 */
		String toHost = consistentHash.getNodeForKey(key);
		if (toHost == null)
			throw new HeliumException("No KVServer is available");
		/**
		 * Serialize the value to be sent over the wire
		 */
		byte[] valueAsBytes = SerializationUtils.serialize(value);
		/**
		 * Create a PUT_REQUEST for the key to the KVServer
		 */
		KVMessage kvmessage = KVMessage.newBuilder()
				.setType(MessageType.PUT_REQUEST)
				.setConsistencyLevel(consistencyLevel).setKey(key)
				.setValue(ByteString.copyFrom(valueAsBytes)).build();
		logger.info("Sending PUT request to {} for Key: {} ", toHost, key);
		KVMessage reply = HeliumSocketUtils.sendAndReceiveKVMessage(kvmessage,
				toHost, kvServerListenPort);
		long after = System.currentTimeMillis();
		logger.info("LATENCY PUT KEY: {} in {} ms Level: {}", key,
				(after - before), consistencyLevel);
		/**
		 * Check if the operation was successful
		 */
		if (reply == null) {
			throw new HeliumException("Connection error");
		}
		if (reply.getType() != MessageType.REPLY_SUCCESS) {
			if (reply.getType() == MessageType.REPLY_FAILURE_DUPLICATE_KEY) {
				HeliumValue replyValue = (HeliumValue) SerializationUtils
						.deserialize(reply.getValue().toByteArray());
				throw new HeliumException(String.format(
						"Key %s is already mapped to value %s", key,
						replyValue.getValue()));
			}
		}
		return;
	}

	/**
	 * Removes the entry for key from the Key Value store
	 * 
	 * @param key
	 * @return Value corresponding to the key
	 * @throws HeliumException
	 */
	public HeliumValue remove(String key, ConsistencyLevel consistencyLevel)
			throws HeliumException {
		long before = System.currentTimeMillis();
		/**
		 * Find out which KVServer is storing this key
		 */
		String toHost = consistentHash.getNodeForKey(key);
		if (toHost == null)
			throw new HeliumException("No KVServer is available");
		/**
		 * Create a DELETE_REQUEST for the key to the KVServer
		 */
		KVMessage kvmessage = KVMessage.newBuilder()
				.setType(MessageType.DELETE_REQUEST).setKey(key)
				.setConsistencyLevel(consistencyLevel).build();
		logger.info("Sending DELETE request to {} for Key: {}", toHost, key);
		KVMessage reply = HeliumSocketUtils.sendAndReceiveKVMessage(kvmessage,
				toHost, kvServerListenPort);
		long after = System.currentTimeMillis();
		logger.info("LATENCY DELETE KEY: {} in {} ms", key, (after - before));
		/**
		 * Check if the operation was successful
		 */
		if (reply == null) {
			throw new HeliumException("Connection error");
		}
		if (reply.getType() != MessageType.REPLY_SUCCESS) {
			if (reply.getType() == MessageType.REPLY_FAILURE_KEY_ERROR)
				throw new HeliumException(String.format(
						"Key %s does not exist in Map", key));
		}
		HeliumValue valueObject = (HeliumValue) SerializationUtils
				.deserialize(reply.getValue().toByteArray());
		return valueObject;
	}

	/**
	 * Updates the value for a key in the Key Value store
	 * 
	 * @param key
	 *            Integer Key
	 * @param value
	 *            New Value for the key
	 * @return Old Value for the key
	 * @throws HeliumException
	 */
	public HeliumValue update(String key, HeliumValue value,
			ConsistencyLevel consistencyLevel) throws HeliumException {
		long before = System.currentTimeMillis();
		/**
		 * Find out which KVServer is storing this key
		 */
		String toHost = consistentHash.getNodeForKey(key);
		if (toHost == null)
			throw new HeliumException("No KVServer is available");
		/**
		 * Serialize the value to be sent over the wire
		 */
		byte[] valueAsBytes = SerializationUtils.serialize(value);
		/**
		 * Create a UPDATE_REQUEST for the key to the KVServer
		 */
		KVMessage kvmessage = KVMessage.newBuilder()
				.setType(MessageType.UPDATE_REQUEST)
				.setConsistencyLevel(consistencyLevel).setKey(key)
				.setValue(ByteString.copyFrom(valueAsBytes)).build();
		logger.info("Sending UPDATE request to {} for Key: {} ", toHost, key);
		KVMessage reply = HeliumSocketUtils.sendAndReceiveKVMessage(kvmessage,
				toHost, kvServerListenPort);
		long after = System.currentTimeMillis();
		logger.info("LATENCY UPDATE KEY: {} in {} ms", key, (after - before));
		/**
		 * Check if the operation was successful
		 */
		if (reply == null) {
			throw new HeliumException("Connection error");
		}
		if (reply.getType() != MessageType.REPLY_SUCCESS) {
			if (reply.getType() == MessageType.REPLY_FAILURE_KEY_ERROR)
				throw new HeliumException(String.format(
						"Key %s does not exist in Map", key));
		}
		/**
		 * return the old value back to user
		 */
		HeliumValue valueObject = (HeliumValue) SerializationUtils
				.deserialize(reply.getValue().toByteArray());
		return valueObject;
	}

	/**
	 * Update the ConsistentHash when a new KVServer joins the group
	 */
	public void nodeJoined(String node, boolean isActive) {
		/**
		 * isActive will be true for KVServers. For other type of members, it
		 * will be false
		 */
		if (!isActive)
			return;
		logger.info("KVServer JOIN: {}  Updating the ConsistentHash", node);
		consistentHash.addNode(node);
	}

	/**
	 * Update the ConsistentHash when a KVServer leaves the group
	 */
	public void nodeLeft(String node, boolean isActive) {
		/**
		 * isActive will be true for KVServers. For other type of members, it
		 * will be false
		 */
		if (!isActive)
			return;
		logger.info("KVServer LEAVE: {} Updating the ConsistentHash", node);
		consistentHash.removeNode(node);
	}

	/**
	 * Update the ConsistentHash when a new KVServer fails
	 */
	public void nodeCrashed(String node, boolean isActive) {
		/**
		 * isActive will be true for KVServers. For other type of members, it
		 * will be false
		 */
		if (!isActive)
			return;
		logger.info("KVServer CRASH: {} Updating the ConsistentHash", node);
		consistentHash.removeNode(node);
	}

}
