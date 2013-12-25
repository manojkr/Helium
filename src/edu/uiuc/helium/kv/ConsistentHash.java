package edu.uiuc.helium.kv;

import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Manoj
 * 
 *         Implementation of Consistent Hash algorithm
 * 
 */
public class ConsistentHash {

	private static final Logger logger = LoggerFactory
			.getLogger(ConsistentHash.class);
	private TreeMap<Integer, String> circle = new TreeMap<Integer, String>();
	private int circleLength;

	/**
	 * @param circleLen
	 *            The size of the circle/ring where keys will be distributed
	 */
	public ConsistentHash(int circleLen) {
		this.circleLength = circleLen;
	}

	/**
	 * 
	 * @param node
	 *            IPAddress of new node that is joining the ring
	 */
	public void addNode(String node) {
		int ringPosition = getRingPosition(node);
		logger.info("Adding node: {} RingPosition: {} ", node, ringPosition);
		circle.put(ringPosition, node);
		logger.info("after {}",circle);
	}

	/**
	 * 
	 * @param node
	 *            IPAddress is node that is leaving the ring
	 */
	public void removeNode(String node) {
		int ringPosition = getRingPosition(node);
		logger.info("Removing node: {} RingPosition: {} ", node, ringPosition);
		circle.remove(ringPosition);
		logger.info("after {}",circle);
	}

	/**
	 * 
	 * Shows all the Hosts that are part of ConsistentHash
	 * and their respective ring positions
	 */
	public TreeMap<Integer, String> showNodePositions() {
		return new TreeMap<Integer, String>(circle);
	}

	/**
	 * 
	 * @param key
	 *            Integer key
	 * @return Node which is responsible for this key
	 */
	public String getNodeForKey(String key) {
		String node = null;
		if (circle.size() > 0) {
			int hashCode = getRingPosition(key);
			Entry<Integer, String> ceilingEntry = circle.ceilingEntry(hashCode);
			if (ceilingEntry != null) {
				node = ceilingEntry.getValue();
				logger.info(
						"Key: {} RingPosition: {} mapped to Node: {} RingPosition: {}",
						key, hashCode, node, ceilingEntry.getKey());
			} else {
				Entry<Integer, String> firstEntry = circle.firstEntry();
				node = firstEntry.getValue();
				logger.info(
						"Key: {} RingPosition: {} mapped to Node: {} RingPosition: {}",
						key, hashCode, node, firstEntry.getKey());
			}
		} else {
			logger.info("ConsistentHash does not contain any nodes");
		}
		return node;
	}

	/**
	 *
	 * Given a hostname, return its successor on the ring
	 */
	public String getSuccessorOfHost(String node) {
		int ringPosition = getRingPosition(node);
		String succ = getSuccessorNodeOnRing(ringPosition);
		/**
		 * Edge case check- a node can't be its own 
		 * successor
		 */
		if (!succ.equals(node))
			return succ;
		else
			return null;
	}

	/**
	 * Given a hostname, return its predecessor on the ring 
	 */
	public String getPredecessorOfHost(String node) {
		int ringPosition = getRingPosition(node);
		String pre = getPredecessorNodeOnRing(ringPosition);
		/**
		 * Edge case check- a node can't be its own 
		 * predecessor
		 */
		if (!pre.equals(node))
			return pre;
		else
			return null;
	}

	/**
	 * Given a ring position, find the Host which is the successor
	 * of the given Ring Position 
	 */
	private String getSuccessorNodeOnRing(Integer selfPosition) {
		String successor = null;
		Integer successorPosition = circle.higherKey(selfPosition);
		/**
		 * If the successor is null, then wrap around the Circular 
		 * ring and return the lowest Key present in the ring
		 */
		if (successorPosition == null) {
			Integer key = circle.firstKey();
			if (key != null) {
				successor = circle.get(key);
			}
		} else {
			successor = circle.get(successorPosition);
		}
		return successor;
	}

	/**
	 * Given a ring position, find the Host which is the predecessor
	 * of the given Ring Position 
	 */
	private String getPredecessorNodeOnRing(Integer selfPosition) {
		String predecessor = null;
		Integer predecessorPosition = circle.lowerKey(selfPosition);
		/**
		 * If the Predecessor is null, then wrap around the Circular 
		 * ring and return the highest Key present in the ring
		 */
		if (predecessorPosition == null) {
			Integer key = circle.lastKey();
			if (key != null) {
				predecessor = circle.get(key);
			}
		} else {
			predecessor = circle.get(predecessorPosition);
		}
		return predecessor;
	}

	/**
	 * 
	 * @param key
	 *            Integer key
	 * @return Ring Postion for the key
	 */
	private int getRingPosition(String key) {
		/**
		 * Use SHA512 for hashing. During our testing, this function
		 * demonstrated good distribution characterstics for Integers
		 */
		String md5Hex = DigestUtils.sha512Hex(key);
		int hashCode = md5Hex.hashCode();

		int ringPosition = (hashCode & 0x7FFFFFFF) % circleLength;
		return ringPosition;
	}

	/**
	 * 
	 * @return All the nodes that are part of ConsistentHash
	 */
	public Set<Integer> getAllNodes() {
		return circle.keySet();
	}

}
