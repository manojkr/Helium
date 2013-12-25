package edu.uiuc.helium.gossip;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.helium.dgrep.SocketUtils;
import edu.uiuc.helium.proto.MessageProtos.GossipMessage;
import edu.uiuc.helium.proto.MessageProtos.GossipMessage.Builder;
import edu.uiuc.helium.proto.MessageProtos.GossipMessage.HostRecord;
import edu.uiuc.helium.proto.MessageProtos.GossipMessage.MessageType;

/**
 * GossipManager is an implementation of Gossip Based Failure Detection
 * protocol. It a) At each tgossip interval, sends Gossip messages to other live
 * nodes b) Using tfail and tcleanup time intervals, it keeps track of hosts
 * which are no longer active and prunes them from active host table
 * 
 * @author Manoj
 * 
 */
public class GossipManager {
	private static final Logger logger = LoggerFactory
			.getLogger(GossipManager.class);

	private long tcleanup;
	private long tfail;
	private long tgossip;
	private Map<String, HostEntry> activeHostTable = new ConcurrentHashMap<String, HostEntry>();
	private Random rand = new Random();
	private String selfIp;
	private SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
	private SimpleDateFormat df1 = new SimpleDateFormat("HH:mm:ss.SSS");
	private int listenPort;
	private List<String> allHosts;
	private double messageLossRate;
	private Map<String, Date> lastJoinTime = new HashMap<String, Date>();
	private int gossip_all_attempts = 3;
	private Date startTime = new Date();
	private Timer failedHostCheckerTask;
	private Timer gossipSenderTask;
	private volatile long sent = 0;

	private boolean monitorBW = false;
	private volatile boolean leaveTheGroup = false;
	private List<MembershipChangeListener> listeners = new ArrayList<MembershipChangeListener>();
	private boolean isActive = true;

	public GossipManager(long tgossip, long tfail, long tcleanup,
			List<String> allHosts, int listenPort, double messageLossRate) {
		this.tgossip = tgossip;
		this.tfail = tfail;
		this.tcleanup = tcleanup;
		this.listenPort = listenPort;
		this.selfIp = SocketUtils.getLocalIpAddress();
		this.allHosts = allHosts;
		this.messageLossRate = messageLossRate;

		activeHostTable.put(selfIp, new HostEntry(selfIp));

		logger.info("Network consists of nedes: {}", allHosts);
		logger.info("Local Ip Address is: {}", selfIp);
	}

	public GossipManager(long tgossip, long tfail, long tcleanup,
			List<String> allHosts, int listenPort, double messageLossRate,
			boolean monitorBW) {
		this(tgossip, tfail, tcleanup, allHosts, listenPort, messageLossRate);
		this.monitorBW = monitorBW;
	}

	public void init() {
		/**
		 * Create a Timer thread for pruning failed hosts entries. It will run
		 * periodically at an interval of tfail millseconds.
		 */
		failedHostCheckerTask = new Timer();
		failedHostCheckerTask.scheduleAtFixedRate(new FailedHostChecker(),
				10000, tfail);

		/**
		 * Create a Timer thread for sending Gossip messages. It will run
		 * periodically at an interval of tgossip milliseconds.
		 */
		gossipSenderTask = new Timer();
		gossipSenderTask.scheduleAtFixedRate(new GossipSender(), 1000, tgossip);

		if (monitorBW) {
			Timer sentBWTask = new Timer();
			sentBWTask.scheduleAtFixedRate(new BWMonitor(), 10000, 60000);
		}
	}

	/**
	 * 
	 * @return the IP Addresses of live hosts that are currently part of
	 *         Distributed System
	 */
	public List<String> getLiveHosts() {
		return new ArrayList<String>(activeHostTable.keySet());
	}

	public List<String> getActiveHosts() {
		List<String> nodes = new ArrayList<String>();
		for (String node : activeHostTable.keySet()) {
			if (activeHostTable.get(node).isActive())
				nodes.add(node);
		}
		return nodes;
	}
	
	public boolean isAlive(String hostname){
		return activeHostTable.containsKey(hostname);
	}

	/**
	 * Updates the local ActiveHostTable based on incoming Gossip Message
	 * 
	 * @param hostRecords
	 *            HostTable received from an incoming Gossip Message
	 */
	public void updateHostEntries(List<HostRecord> hostRecords) {
		long currentTime = System.currentTimeMillis();
		for (HostRecord newRecord : hostRecords) {
			String hostname = newRecord.getHostname();
			/**
			 * Do not use incoming gossip message to update entries for self.
			 */
			if (hostname.equals(selfIp))
				continue;
			/**
			 * Retrieves the new counter value for host H1
			 */
			int counter = newRecord.getCounter();
			boolean isActive = newRecord.getIsActive();

			/**
			 * If an entry for this host does not exist, it means that this is a
			 * new host that has just become part of Distributed System
			 */
			HostEntry oldEntry = activeHostTable.get(hostname);
			if (oldEntry == null) {
				/**
				 * Aha...we have got a new host entering in the group
				 */
				HostEntry newHost = new HostEntry(hostname, counter);
				newHost.updateTime(currentTime);
				newHost.setActive(isActive);
				activeHostTable.put(hostname, newHost);
				for (MembershipChangeListener listener : listeners) {
					listener.nodeJoined(hostname, isActive);
				}
				if (lastJoinTime.get(hostname) == null) {
					lastJoinTime.put(hostname,
							new Date(newRecord.getMachineId()));
					logger.info("{}/{} UPDATE: Node JOIN Ip: {} JoinTime: {}",
							selfIp, startTime.getTime(), hostname,
							lastJoinTime.get(hostname));
				} else {
					Date latestJoinTime = new Date(newRecord.getMachineId());
					logger.info(
							"{}/{} UPDATE: Node REJOIN Ip: {} PreviousJoinTime: {} NewJoinTime: {}",
							selfIp, startTime.getTime(), hostname,
							lastJoinTime.get(hostname), latestJoinTime);
					lastJoinTime.put(hostname, latestJoinTime);
				}

			} else if (counter > oldEntry.getCounter()) {
				/**
				 * If the new received counter is greater than current current,
				 * update the counter. This indicates that the this host is live
				 * in network
				 */

				logger.debug(
						"UPDATE: Node COUNTER_UPDATE Ip: {} OldCounter: {} NewCounter: {}, currentTime: {}",
						hostname, oldEntry.getCounter(), counter,
						df1.format(new Date(currentTime)));

				oldEntry.setCounter(counter);
				oldEntry.updateTime(currentTime);
			}
		}

	}

	/**
	 * Prune the failed hosts from the ActiveHostTable
	 */
	public void deleteFailedHostEntries() {
		long currentTime = System.currentTimeMillis();
		List<String> toRemove = new ArrayList<String>();
		for (String host : activeHostTable.keySet()) {
			/**
			 * You can't remove your entry. You are here at this point of code
			 * means that you are live
			 */
			if (host.equals(selfIp))
				continue;
			HostEntry entry = activeHostTable.get(host);
			long lastUpdateTime = entry.getLastUpdateTime();
			/**
			 * If the host entry is older than tfail+tcleanup time, then we can
			 * assume that host has failed. Remove this host from Active host
			 * table
			 */
			if (lastUpdateTime + tfail + tcleanup < currentTime) {
				logger.info(
						"{}/{} UPDATE: Node DELETE Ip: {} CurrentTime: {} MostRecentGossipTime: {}",
						selfIp, startTime.getTime(), host,
						df.format(new Date(currentTime)),
						df.format(new Date(lastUpdateTime)));
				toRemove.add(host);
			} else if (lastUpdateTime + tfail < currentTime) {
				/**
				 * If the host entry is older than tfail time, then mark this
				 * host for deletion but do not delete that. The reason is to
				 * wait for sufficient time to ensure that other hosts that are
				 * part of Distributed System have also received the information
				 * about failed host
				 */
				logger.info(
						"HostEntry {} is marked for deletion. CurrentTime: {} HostTime: {}",
						host, df.format(new Date(currentTime)),
						df.format(new Date(lastUpdateTime)));
				entry.setRemovable(true);
			}
		}

		for (String host : toRemove) {
			HostEntry removedEntry = activeHostTable.remove(host);
			for (MembershipChangeListener listener : listeners) {
				listener.nodeCrashed(host, removedEntry.isActive());
			}
		}
	}

	public void leaveTheGroup(String host) {
		HostEntry removedEntry = activeHostTable.remove(host);
		if (removedEntry != null) {
			for (MembershipChangeListener listener : listeners) {
				listener.nodeLeft(host, removedEntry.isActive());
			}
			logger.info(
					"{}/{} UPDATE: Node LEAVE_GROUP Ip: {} CurrentTime: {} ",
					selfIp, startTime.getTime(), host, df.format(new Date()));
		}
	}

	/**
	 * Sends gossip message containing Live Host Information to all the nodes
	 * that are currently part of Distributed System
	 */
	public void sendGossips() {
		/**
		 * first increment self's counter
		 */
		HostEntry selfEntry = activeHostTable.get(selfIp);
		if (selfEntry != null)
			selfEntry.incrementCounter();

		Builder newBuilder = GossipMessage.newBuilder();
		newBuilder.setType(MessageType.GOSSIP);
		List<String> hosts = new ArrayList<String>();

		for (String host : activeHostTable.keySet()) {
			/**
			 * For first n = gossip_all_attempts, send Gossip messages to all
			 * hosts that are configured to be part of Distributed System. The
			 * reason is that if any of this is live, it will reply back with
			 * GOSSIP message. That gossip message will contain latest Live
			 * Hosts information.
			 * 
			 * After first n attempts, we should have established which nodes
			 * are live and henceforth need to send gossip messages to only
			 * those hosts
			 */
			HostEntry entry = activeHostTable.get(host);
			if (entry != null) {
				if (gossip_all_attempts <= 0)
					hosts.add(host);
				int counter = entry.getCounter();
				boolean isActive = entry.isActive();
				/**
				 * If a Host is marked as failed, then do not advertise its
				 * details in Gossip Message
				 */
				if (!activeHostTable.get(host).isRemovable()) {
					newBuilder.addHostRecord(HostRecord.newBuilder()
							.setHostname(host).setCounter(counter)
							.setIsActive(isActive)
							.setMachineId(startTime.getTime()).build());
				}
			}
		}
		if (gossip_all_attempts > 0) {
			hosts.addAll(allHosts);
			gossip_all_attempts--;
		}
		hosts.remove(selfIp);

		GossipMessage gossipMessage = newBuilder.build();
		int nHosts = hosts.size();
		/**
		 * If live hosts count is greater than 15, then randomly choose one
		 * fifth of the hosts and send gossips to only those hosts.
		 * 
		 * For less than 15 hosts ie. smaller networks, we can send gossips to
		 * all hosts without congesting the network
		 */
		if (hosts.size() > 15)
			nHosts = hosts.size() / 5;

		for (int i = 0; i < nHosts; i++) {
			int nextHost = hosts.size() > 15 ? rand.nextInt(hosts.size()) : i;
			if (rand.nextDouble() <= messageLossRate) {
				logger.info("Dropping the gossip message for {}", hosts.get(i));
				continue;
			}

			if (!leaveTheGroup) {
				sendMessage(hosts.get(i), listenPort, gossipMessage);
			} else {
				GossipMessage msg = GossipMessage
						.newBuilder()
						.setType(MessageType.GROUP_LEAVE)
						.addHostRecord(
								HostRecord.newBuilder().setHostname(selfIp)
										.setMachineId(startTime.getTime())
										.setCounter(selfEntry.getCounter()))
						.build();
				sendMessage(hosts.get(i), listenPort, msg);
			}
		}

	}

	/**
	 * Sends GossipMessage to a host at listening at port
	 * 
	 * @param host
	 * @param listenPort
	 * @param gossipMessage
	 */
	private void sendMessage(String host, int listenPort,
			GossipMessage gossipMessage) {
		try {
			DatagramSocket socket = new DatagramSocket();
			byte[] replyMessage = gossipMessage.toByteArray();
			InetSocketAddress toAddress = new InetSocketAddress(host,
					listenPort);
			DatagramPacket replyPacket = new DatagramPacket(replyMessage, 0,
					replyMessage.length, toAddress);

			logger.debug(
					"Sending gossip message to host: {} port: {} length: {}",
					host, listenPort, replyPacket.getLength());
			sent = sent + replyPacket.getLength();
			socket.send(replyPacket);
			socket.close();
		} catch (IOException e) {
			logger.error("Error sending gossip message to host: {}", host);
		}
	}

	private void printBWSent() {
		logger.info("Bytes sent: {} bytes", sent);
		sent = 0;
	}

	/**
	 * A periodic task for pruning failed host entries
	 * 
	 */
	class FailedHostChecker extends TimerTask {

		@Override
		public void run() {
			deleteFailedHostEntries();
		}

	}

	/**
	 * A periodic task for sending Gossip messages containing live host
	 * information
	 * 
	 */
	class GossipSender extends TimerTask {

		@Override
		public void run() {
			sendGossips();
		}

	}

	class BWMonitor extends TimerTask {

		@Override
		public void run() {
			printBWSent();
		}

	}

	public void leave() {
		leaveTheGroup = true;
	}

	public void registerMembershipChangeListener(
			MembershipChangeListener listener) {
		listeners.add(listener);
	}

	public void setIsActive(boolean isActive) {
		this.isActive = isActive;
		activeHostTable.get(selfIp).setActive(isActive);
	}

}
