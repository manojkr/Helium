package edu.uiuc.helium.kv;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.uiuc.helium.dgrep.SocketUtils;
import edu.uiuc.helium.gossip.FailureMonitor;
import edu.uiuc.helium.gossip.HeliumConfig;
import edu.uiuc.helium.gossip.MembershipChangeListener;
import edu.uiuc.helium.proto.MessageProtos.KVMessage;
import edu.uiuc.helium.proto.MessageProtos.KVMessage.ConsistencyLevel;
import edu.uiuc.helium.proto.MessageProtos.KVMessage.MessageType;

/**
 * Key Value Server node. This runs as a process and is responsible for storing
 * Key-Value pairs and handling requests
 * 
 * @author Manoj
 * 
 */
public class HeliumServer implements Runnable, MembershipChangeListener {

	private static final Logger logger = LoggerFactory
			.getLogger(HeliumServer.class);

	private int listenPort;
	private ServerSocket socket;
	private SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss.SSS");
	/**
	 * Data Structure to store the (key,value) pairs. It is thread safe so that
	 * multiple threads can operate at a time
	 */
	private Map<String, HeliumValue> kvStore = new ConcurrentHashMap<String, HeliumValue>();
	private FailureMonitor fm;
	private HeliumConfig config;
	private ConsistentHash consistentHash;
	private String selfip;
	/**
	 * Stores the hosts that are part of this node's replica group
	 */
	private Set<String> replicaGroup = Collections
			.synchronizedSet(new HashSet<String>());
	/**
	 * Stores the keys for which the current node is co-ordinator
	 */
	private List<String> selfKeys = Collections
			.synchronizedList(new ArrayList<String>());

	private LinkedList<String> readCommands = new LinkedList<String>();
	private LinkedList<String> writeCommands = new LinkedList<String>();
	private RepairProtocol rp;

	public HeliumServer(HeliumConfig config) {
		this.listenPort = config.getKVServerListenPort();
		this.config = config;
		this.consistentHash = new ConsistentHash(config.getKVServerCircleSize());
		this.selfip = SocketUtils.getLocalIpAddress();
		this.rp = new RepairProtocol(listenPort);
		consistentHash.addNode(selfip);

	}

	/**
	 * Initialize the KVServer
	 */
	public void init() {
		/**
		 * Initialize the FailureMonitor thread. This is required for Membership
		 * detection to know which KVServers are part of group
		 */
		logger.info("Initializing KVServer");
		fm = new FailureMonitor(config);
		fm.registerMembershipChangeListener(this);
		/**
		 * Start the repair protocol thread
		 */
		Thread rpThread = new Thread(rp);
		rpThread.start();
		/**
		 * Start the failure monitor thread
		 */
		Thread fmThread = new Thread(fm);
		fmThread.start();

	}

	/**
	 * @return Returns all the (key,value) pairs stored in KVStore as a string
	 */
	public String showData() {
		StringBuilder out = new StringBuilder();
		for (String key : kvStore.keySet()) {
			out.append(String.format("Key: %s Value: %s IsCoordinator: %s \n",
					key, kvStore.get(key).getValue(),
					selfKeys.contains((Object) key)));
		}
		return out.toString();
	}

	/**
	 * 
	 * @return Returns all the KVServers that are part of group as a string
	 */
	public String showAllKVServers() {
		return fm.getActiveLiveHosts().toString();
	}

	public String showReplicaGroup() {
		return replicaGroup.toString();
	}

	public void run() {
		try {
			socket = new ServerSocket(listenPort);
		} catch (IOException e) {
			logger.error("Failed to listen on port {}", listenPort, e);
			return;
		}

		logger.info("Started listening on port: {}", listenPort);

		while (true) {
			try {
				logger.info("Waiting for new KeyValue Requests");
				Socket connection = socket.accept();
				/**
				 * For each incoming request, create a GrepRequestHandler task
				 * and submit the task to ThreadPool for execution
				 */
				handleKVRequest(connection);
			} catch (IOException e) {
				logger.error(
						"Error occured with listening for new GrepRequests", e);
			}

		}

	}

	/**
	 * Handle an incoming request
	 * 
	 * @param socket
	 */
	private void handleKVRequest(Socket socket) {
		try {
			logger.info("Starting processing KVRequest from {}",
					socket.getInetAddress());
			/**
			 * Create streams for reading to and from socket
			 */
			BufferedInputStream inputStream = new BufferedInputStream(
					socket.getInputStream());
			BufferedOutputStream outputStream = new BufferedOutputStream(
					socket.getOutputStream());
			/**
			 * Reads the KVMessage from the InputStream of socket.
			 */
			KVMessage request = KVMessage.newBuilder().mergeFrom(inputStream)
					.build();
			String requestKey = request.getKey();
			ConsistencyLevel requestConsistencyLevel = request
					.getConsistencyLevel();
			ByteString requestV = request.getValue();
			HeliumValue requestValue = null;
			if (requestV.size() > 0)
				requestValue = (HeliumValue) SerializationUtils
						.deserialize(requestV.toByteArray());
			String replyKey = requestKey;
			HeliumValue replyHelenusValue = null;
			MessageType replyType;
			switch (request.getType()) {
			case GET_REQUEST:
				/**
				 * Logic for lookup request
				 */
				logger.info("GET_REQUEST Key: {} ConsistencyLevel: {}",
						requestKey, requestConsistencyLevel);
				updateReadCommands(requestKey);
				replyHelenusValue = kvStore.get(requestKey);
				switch (requestConsistencyLevel) {
				case ALL:
				case QUORUM:
					if (replyHelenusValue != null) {
						KVMessage succMsg = KVMessage.newBuilder()
								.setType(MessageType.GET_REQUEST)
								.setKey(requestKey)
								.setConsistencyLevel(ConsistencyLevel.NONE)
								.build();
						for (String node : replicaGroup) {
							KVMessage succReply = HeliumSocketUtils
									.sendAndReceiveKVMessage(succMsg, node,
											listenPort);
							if (succReply != null) {
								HeliumValue temp = (HeliumValue) SerializationUtils
										.deserialize(succReply.getValue()
												.toByteArray());
								if (temp.getTimestamp() > replyHelenusValue
										.getTimestamp())
									replyHelenusValue = temp;
							}
						}
					}
					break;
				}
				if (replyHelenusValue == null)
					replyType = MessageType.REPLY_FAILURE_KEY_ERROR;
				else
					replyType = MessageType.REPLY_SUCCESS;
				break;

			case PUT_REQUEST:
				/**
				 * logic for INSERT request type
				 */
				Date putRequestTime = new Date();
				logger.info("PUT_REQUEST Key: {} Timestamp: {}", requestKey,
						df.format(putRequestTime));
				if (requestValue == null)
					replyType = MessageType.REPLY_FAILURE_INVALID_REQUEST;
				else if (kvStore.containsKey(requestKey)
						&& requestConsistencyLevel != ConsistencyLevel.NONE) {
					replyType = MessageType.REPLY_FAILURE_DUPLICATE_KEY;
					replyHelenusValue = kvStore.get(requestKey);
				} else {
					updateWriteCommands("Insert", requestKey,
							requestValue.getValue());
					HeliumValue val = new HeliumValue(
							requestValue.getValue(), putRequestTime.getTime());
					replyHelenusValue = kvStore.put(requestKey, val);
					if (selfip.equals(consistentHash.getNodeForKey(requestKey)))
						selfKeys.add(requestKey);
					switch (requestConsistencyLevel) {
					case ALL:
					case QUORUM:
					case ONE:
						KVMessage succMsg = KVMessage
								.newBuilder()
								.setType(MessageType.PUT_REQUEST)
								.setKey(requestKey)
								.setConsistencyLevel(ConsistencyLevel.NONE)
								.setValue(
										ByteString.copyFrom(SerializationUtils
												.serialize(val))).build();
						for (String node : replicaGroup) {
							if (requestConsistencyLevel == ConsistencyLevel.ONE) {
								rp.queueMessage(new AsyncUpdateMessage(succMsg,
										node));
							} else {
								HeliumSocketUtils.sendAndReceiveKVMessage(
										succMsg, node, listenPort);
							}
						}
					}
					replyType = MessageType.REPLY_SUCCESS;
				}
				break;
			case UPDATE_REQUEST:
				Date updateRequestTime = new Date();
				logger.info("UPDATE_REQUEST Key: {}  Timestamp: {}",
						requestKey, df.format(updateRequestTime));
				updateWriteCommands("Update", requestKey,
						requestValue.getValue());
				if (requestValue == null)
					replyType = MessageType.REPLY_FAILURE_INVALID_REQUEST;
				else if (!kvStore.containsKey(requestKey)) {
					replyType = MessageType.REPLY_FAILURE_KEY_ERROR;
				} else {
					HeliumValue val = new HeliumValue(
							requestValue.getValue(),
							updateRequestTime.getTime());
					replyHelenusValue = kvStore.put(requestKey, val);
					switch (requestConsistencyLevel) {
					case ALL:
					case QUORUM:
					case ONE:
						KVMessage succMsg = KVMessage
								.newBuilder()
								.setType(MessageType.UPDATE_REQUEST)
								.setKey(requestKey)
								.setConsistencyLevel(ConsistencyLevel.NONE)
								.setValue(
										ByteString.copyFrom(SerializationUtils
												.serialize(val))).build();
						for (String node : replicaGroup) {
							if (requestConsistencyLevel == ConsistencyLevel.ONE) {
								rp.queueMessage(new AsyncUpdateMessage(succMsg,
										node));
							} else {
								HeliumSocketUtils.sendAndReceiveKVMessage(
										succMsg, node, listenPort);
							}
						}
					}
					replyType = MessageType.REPLY_SUCCESS;
				}
				break;
			case DELETE_REQUEST:
				logger.info("DELETE_REQUEST Key: {}", requestKey);
				updateWriteCommands("DELETE", requestKey, "NA");
				if (!kvStore.containsKey(requestKey)) {
					replyType = MessageType.REPLY_FAILURE_KEY_ERROR;
				} else {
					if (selfip.equals(consistentHash.getNodeForKey(requestKey)))
						selfKeys.remove((Object) requestKey);
					replyHelenusValue = kvStore.remove(requestKey);
					if (replyHelenusValue != null
							&& requestConsistencyLevel != ConsistencyLevel.NONE) {
						KVMessage succMsg = KVMessage.newBuilder()
								.setType(MessageType.DELETE_REQUEST)
								.setKey(requestKey)
								.setConsistencyLevel(ConsistencyLevel.NONE)
								.build();
						for (String node : replicaGroup) {
							if (requestConsistencyLevel == ConsistencyLevel.ONE) {
								rp.queueMessage(new AsyncUpdateMessage(succMsg,
										node));
							} else {
								KVMessage succReply = HeliumSocketUtils
										.sendAndReceiveKVMessage(succMsg, node,
												listenPort);
								HeliumValue temp = (HeliumValue) SerializationUtils
										.deserialize(succReply.getValue()
												.toByteArray());
								if (temp.getTimestamp() > replyHelenusValue
										.getTimestamp())
									replyHelenusValue = temp;
							}
						}
					}
					replyType = MessageType.REPLY_SUCCESS;
				}
				break;
			default:
				logger.info("INVALID_REQUEST Key: {}", requestKey);
				replyType = MessageType.REPLY_FAILURE_INVALID_REQUEST;
			}
			/**
			 * Create a reply message indicating SUCCESS/FAILURE of opearation
			 * and result
			 */
			byte[] replyValueAsBytes = SerializationUtils
					.serialize(replyHelenusValue);
			KVMessage reply = KVMessage.newBuilder().setType(replyType)
					.setKey(replyKey)
					.setConsistencyLevel(ConsistencyLevel.NONE)
					.setValue(ByteString.copyFrom(replyValueAsBytes)).build();
			outputStream.write(reply.toByteArray());
			outputStream.flush();
			socket.close();
		} catch (IOException e) {
			logger.error("Error occurred while processing GrepRequest from {}",
					socket.getInetAddress(), e);
		}
	}

	public static void main(String[] args) {
		/**
		 * Read the config file and read required variables
		 */
		String configFileName = System.getProperty("app.config");
		HeliumConfig config = new HeliumConfig(configFileName);
		try {
			config.init();
		} catch (Exception e) {
			logger.error("Error initializing config for GrepServer", e);
			System.exit(-1);
		}

		/**
		 * Having read the config file,Start the Grep Server Thead now
		 */
		HeliumServer kv = new HeliumServer(config);
		logger.info("Starting new thread for KVServer");
		Thread kvServerThread = new Thread(kv);
		kvServerThread.run();
		logger.info("Exiting now");
		return;
	}

	/**
	 * Adjust the replication when a new node joins
	 */
	public void nodeJoined(String joinedNode, boolean isActive) {
		if (isActive) {
			logger.info("New KVServer: {} has joined", joinedNode);
			logger.info("Replication group. Before - {}", replicaGroup);
			consistentHash.addNode(joinedNode);

			String predecessor = consistentHash.getPredecessorOfHost(selfip);
			String successor = consistentHash.getSuccessorOfHost(selfip);
			logger.info(
					"SelfIp: {} JoinedNode: {} Successor: {} Predecessor: {}",
					selfip, joinedNode, successor, predecessor);
			String succToSuccessor = null;
			if (successor != null) {
				succToSuccessor = consistentHash.getSuccessorOfHost(successor);
			}
			/**
			 * If the new joined node is predecessor, then that new node becomes
			 * co-ordinator for some of the keys
			 */
			if (joinedNode.equals(predecessor)) {
				logger.info("Joined node {} is predecessor to node {}",
						joinedNode, selfip);
				List<String> keysToMove = new ArrayList<String>();
				synchronized (selfKeys) {
					Iterator i = selfKeys.iterator(); // Must be in synchronized
														// block
					while (i.hasNext()) {
						String k = (String) i.next();
						String node = consistentHash.getNodeForKey(k);
						if (joinedNode.equals(node)) {
							keysToMove.add(k);
						}
					}
				}
				logger.info("Removing {} from self keys", keysToMove);
				for (String k : keysToMove) {
					selfKeys.remove(k);
				}
				logger.info("Will move keys: {} to node {} and delete from {}",
						keysToMove, joinedNode, succToSuccessor);
				for (String k : keysToMove) {
					moveKey(k, kvStore.get(k), joinedNode);
					deleteKey(k, succToSuccessor);
				}
			}
			/**
			 * If new node happens to be in its replica group, send him the keys
			 * corresponding to self replica group
			 */
			if (joinedNode.equals(successor)
					|| joinedNode.equals(succToSuccessor)) {
				logger.info("Joined node {} is a new successor to node {}",
						joinedNode, selfip);
				String nodeToDelete = null;
				for (String node : replicaGroup) {
					if (!(node.equals(succToSuccessor) || node
							.equals(successor)))
						nodeToDelete = node;
				}
				logger.info("Will move keys: {} to node {} and delete from {}",
						selfKeys, joinedNode, nodeToDelete);
				synchronized (selfKeys) {
					Iterator i = selfKeys.iterator(); // Must be in synchronized
														// block
					while (i.hasNext()) {
						String key = (String) i.next();
						moveKey(key, kvStore.get(key), joinedNode);
						deleteKey(key, nodeToDelete);
					}
				}
				replicaGroup.remove(nodeToDelete);
				replicaGroup.add(joinedNode);
			}

			logger.info("Replication group. After - {}", replicaGroup);
		}
	}

	/**
	 * Moves the (key, value) pair to KVServer node
	 * 
	 * @param key
	 * @param value
	 * @param node
	 *            IPAddress of the node to be moved
	 * @return
	 */
	private boolean moveKey(String key, HeliumValue value, String node) {
		if (node != null && !node.equals(selfip) && value != null) {
			byte[] valueAsBytes = SerializationUtils.serialize(value);
			KVMessage msg = KVMessage.newBuilder()
					.setType(MessageType.PUT_REQUEST).setKey(key)
					.setValue(ByteString.copyFrom(valueAsBytes))
					.setConsistencyLevel(ConsistencyLevel.NONE).build();
			logger.info("Moving Key: {} to Node: {}", key, node);
			KVMessage reply = HeliumSocketUtils.sendAndReceiveKVMessage(msg,
					node, listenPort);
			logger.info("Received {}", reply);
			if (reply == null) {
				logger.error("Connection error");
				return false;
			} else if (reply.getType() != MessageType.REPLY_SUCCESS) {
				logger.error("Could not move key: {} value: {} to node: {}",
						key, value, node);
				return false;
			} else {
				logger.info("Successfully moved Key: {} Value: {} to Node: {}",
						key, value, node);
			}
		} else {
			logger.info("Skipping because node is {}", node);
		}
		return true;
	}

	/**
	 * Delete the key from the host node
	 */
	private boolean deleteKey(String key, String node) {
		if (node != null && !node.equals(selfip)) {
			KVMessage msg = KVMessage.newBuilder()
					.setType(MessageType.DELETE_REQUEST).setKey(key)
					.setConsistencyLevel(ConsistencyLevel.NONE).build();
			logger.info("Delete Key: {} on Node: {}", key, node);
			KVMessage reply = HeliumSocketUtils.sendAndReceiveKVMessage(msg,
					node, listenPort);
			if (reply == null) {
				logger.error("Connection error");
				return false;
			} else if (reply.getType() != MessageType.REPLY_SUCCESS) {
				logger.error(
						"Could not delete key: {} on the node: {} Type: {}",
						key, node, reply.getType());
				return false;
			} else {
				logger.info("Successfully deleted Key: {} on the Node: {}",
						key, node);
			}
		} else {
			logger.info("Skipping because node is {}", node);
		}
		return true;
	}

	/**
	 * Updates the ConsistentHash if some KVServer has left the Group
	 */
	public void nodeLeft(String leftNode, boolean isActive) {
		if (isActive) {
			logger.info("Node: {} has left", leftNode);
			logger.info("Replication group. Before - {}", replicaGroup);
			adjustReplication(leftNode);
			logger.info("Replication group. After - {}", replicaGroup);
		}
	}

	/**
	 * Updates the ConsistentHash if some KVServer has crashed
	 */
	public void nodeCrashed(String crashedNode, boolean isActive) {
		if (isActive) {
			logger.info("Node: {} has crashed", crashedNode);
			logger.info("Replication group. Before - {}", replicaGroup);
			adjustReplication(crashedNode);
			logger.info("Replication group. After - {}", replicaGroup);
		}
	}

	private void adjustReplication(String crashedNode) {
		logger.info("Adjusting replication because {} is no longer alive",
				crashedNode);
		/**
		 * If the crashedNode is in its replica group, then remove it from
		 * replicaGroup and choose a new replica
		 */
		if (replicaGroup.contains(crashedNode)) {
			logger.info(
					"Crashed node {} was part of replica group. Removing now",
					crashedNode);
			replicaGroup.remove(crashedNode);
			consistentHash.removeNode(crashedNode);
			logger.info("Current replica group is {}", replicaGroup);
			String newReplicaCandidate = consistentHash
					.getSuccessorOfHost(selfip);
			if (newReplicaCandidate != null) {
				while (replicaGroup.contains(newReplicaCandidate)
						|| !fm.isAlive(newReplicaCandidate)) {
					logger.info("{} is not suitable as a Replica Group Member",
							newReplicaCandidate);
					newReplicaCandidate = consistentHash
							.getSuccessorOfHost(newReplicaCandidate);
				}
			}
			if (selfip.equals(newReplicaCandidate)) {
				logger.info("I am rotating in a circle");
			}
			if (newReplicaCandidate != null
					&& !newReplicaCandidate.equals(selfip)) {
				logger.info(
						"Found new Replica Group member: {}. Adding it to the Replica Group",
						newReplicaCandidate);
				replicaGroup.add(newReplicaCandidate);
				logger.info("Starting moving self keys to {}",
						newReplicaCandidate);
				synchronized (selfKeys) {
					Iterator i = selfKeys.iterator(); // Must be in synchronized
														// block
					while (i.hasNext()) {
						String key = (String) i.next();
						moveKey(key, kvStore.get(key), newReplicaCandidate);
					}
				}

			}

		} else {
			/*
			 * If the crashed node was predecessor of current node, then Current
			 * Node becomes coordinator for the Key Set of Crashed Node as well.
			 * It will need to make sure that those keys are propagated to its
			 * replicas as well
			 */
			if (amITheNewCoordinator(crashedNode)) {
				List<String> keysToMove = new ArrayList<String>();
				for (String key : kvStore.keySet()) {
					/**
					 * Identify the keys to be moved
					 */
					String nodeForKey = consistentHash.getNodeForKey(key);
					if (crashedNode.equals(nodeForKey)) {
						keysToMove.add(key);
					}
				}
				/**
				 * Remove the crashed node from the ring.
				 */
				consistentHash.removeNode(crashedNode);
				logger.info("Adding {} to selfkeys", keysToMove);
				for (String k : keysToMove) {
					selfKeys.add(k);
				}
				for (String succ : replicaGroup) {
					for (String k : keysToMove) {
						moveKey(k, kvStore.get(k), succ);
					}
				}

			}
		}
		consistentHash.removeNode(crashedNode);
	}

	/**
	 * Checks whether current Node is the new Co-ordinator if crashedNode has
	 * crashed.
	 */
	private boolean amITheNewCoordinator(String crashedNode) {
		boolean amITheNewCoordinator = false;
		String successorOfCrashedHost = consistentHash
				.getSuccessorOfHost(crashedNode);
		logger.info(
				"Checking if I am the new coordinator after {}. Successor: {} ",
				crashedNode, successorOfCrashedHost);
		/**
		 * checks if the current node is successor of the crashedNode, then it
		 * is the new Coordinator
		 */
		if (successorOfCrashedHost != null) {
			if (selfip.equals(successorOfCrashedHost))
				amITheNewCoordinator = true;
			else if (!fm.isAlive(successorOfCrashedHost)) {
				successorOfCrashedHost = consistentHash
						.getSuccessorOfHost(successorOfCrashedHost);
				logger.info("Super successor: {}", successorOfCrashedHost);
				if (selfip.equals(successorOfCrashedHost))
					amITheNewCoordinator = true;
			}
		}
		logger.info("I am the new coordinator is {} after {}",
				amITheNewCoordinator, crashedNode);
		return amITheNewCoordinator;
	}

	/**
	 * The method is invoked when KVServer wants to leave the group. When a
	 * KVServer leaves the group, it needs to move any keys that it is storing
	 * to its successor in the Ring.
	 */
	public void leave() {
		logger.info("Leaving the group");
		fm.leave();
	}

	/**
	 * Add the given command to last 10 Read commands
	 */
	private void updateReadCommands(String key) {
		synchronized (readCommands) {
			if (readCommands.size() > 10) {
				readCommands.removeFirst();
			}
			readCommands.addLast(String.format("Lookup Key: %s Time: %s", key,
					new Date()));
		}

	}

	/**
	 * Add the given command to last 10 Write commands
	 */
	private void updateWriteCommands(String type, String key, Object value) {
		synchronized (writeCommands) {
			if (writeCommands.size() > 10) {
				writeCommands.removeFirst();
			}
			writeCommands.addLast(String.format(
					"Type: %s Key: %s Value: %s Time: %s", type, key, value,
					new Date()));
		}
	}

	/**
	 * @return Returns the last 10 read commands as a String
	 */
	public String getReadCommands() {
		StringBuilder out = new StringBuilder();
		synchronized (readCommands) {
			for (String c : readCommands) {
				out.append(c);
				out.append("\n");
			}
		}
		return out.toString();
	}

	/**
	 * @return Returns the last 10 write commands as a String
	 */
	public String getWriteCommands() {
		StringBuilder out = new StringBuilder();
		synchronized (writeCommands) {
			for (String c : writeCommands) {
				out.append(c);
				out.append("\n");
			}
		}
		return out.toString();
	}

}
