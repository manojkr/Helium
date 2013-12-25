package edu.uiuc.helium.gossip;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.helium.proto.MessageProtos.GossipMessage;
import edu.uiuc.helium.proto.MessageProtos.GossipMessage.Builder;
import edu.uiuc.helium.proto.MessageProtos.GossipMessage.HostRecord;
import edu.uiuc.helium.proto.MessageProtos.GossipMessage.MessageType;

/**
 * FailureMonitor monitors the live hosts in the Distributed System. a) It uses
 * a Gossip Based Failure Detection approach to keep track of live hosts that
 * are part of system. b) It listens over socket and provides clients with a
 * lookup service where clients can get the list of all the live hosts are
 * currently part of System
 * 
 */
public class FailureMonitor implements Runnable {
	private static final Logger logger = LoggerFactory
			.getLogger(FailureMonitor.class);

	private static HeliumConfig config;
	private static FailureMonitor fm;
	private int listenPort;
	private GossipManager gm;
	private byte[] receivedMessageBuffer = new byte[10000];
	private volatile long received = 0;
	private boolean monitorBW = false;
	private boolean isActive = true;

	public FailureMonitor(int listenPort, long tgossip, long tfail,
			long tcleanup, List<String> allHosts, double messageLossRate) {
		gm = new GossipManager(tgossip, tfail, tcleanup, allHosts, listenPort,
				messageLossRate);
		gm.init();
		this.listenPort = listenPort;
	}

	public FailureMonitor(HeliumConfig config) {
		gm = new GossipManager(config.getGossipInterval(), config.gettFail(),
				config.gettCleanup(), config.getHosts(),
				config.getListenPort(), config.getMessageLossRate());
		gm.init();
		this.listenPort = config.getListenPort();
	}

	public FailureMonitor(HeliumConfig config, boolean isActive) {
		this.isActive = isActive;
		gm = new GossipManager(config.getGossipInterval(), config.gettFail(),
				config.gettCleanup(), config.getHosts(),
				config.getListenPort(), config.getMessageLossRate());
		gm.setIsActive(isActive);
		gm.init();
		this.listenPort = config.getListenPort();
	}

	public FailureMonitor(int listenPort, long tgossip, long tfail,
			long tcleanup, List<String> allHosts, double messageLossRate,
			boolean monitorBW) {
		this(listenPort, tgossip, tfail, tcleanup, allHosts, messageLossRate);
		this.monitorBW = monitorBW;
	}

	public void run() {
		logger.info("Failure Monitor thread is starting now");
		if (monitorBW) {
			Timer receivedBWTask = new Timer();
			receivedBWTask.scheduleAtFixedRate(new BWMonitor(), 60 * 1000,
					10000);
		}

		DatagramSocket serverSocket = null;
		DatagramSocket replySocket = null;
		try {
			/**
			 * Create a Server Socket to listen for a) The LIVE_HOSTS_REQUEST -
			 * clients can ask for live hosts are part of system b) GOSSIP
			 * message - other hosts that are part of Distributed System send
			 * GOSSIP messages.
			 */
			logger.info("Listening on port {}", listenPort);
			serverSocket = new DatagramSocket(listenPort);
			replySocket = new DatagramSocket();
		} catch (IOException e) {
			logger.error("Error while creating Server socket on port "
					+ listenPort + " . Exiting now", e);
			return;
		}
		while (true) {
			try {
				logger.debug("Waiting for next gossip message");
				DatagramPacket receivedPacket = new DatagramPacket(
						receivedMessageBuffer, receivedMessageBuffer.length);
				serverSocket.receive(receivedPacket);
				received += receivedPacket.getLength();
				byte[] receivedData = receivedPacket.getData();

				Builder inMessageBuilder = GossipMessage.newBuilder();
				inMessageBuilder.mergeFrom(receivedData,
						receivedPacket.getOffset(), receivedPacket.getLength());
				GossipMessage inmsg = inMessageBuilder.build();
				switch (inmsg.getType()) {
				case LIVE_HOSTS_REQUEST:
					logger.info("Received LIVE_HOSTS_REQUEST from {}",
							serverSocket.getInetAddress());
					/**
					 * Create a reply message containing the Live hosts in
					 * Distributed System
					 */
					List<String> liveHosts = gm.getLiveHosts();
					GossipMessage reply = GossipMessage.newBuilder()
							.setType(MessageType.LIVE_HOSTS_REPLY)
							.addAllLiveHosts(liveHosts).build();
					byte[] replyByteArray = reply.toByteArray();
					DatagramPacket replyPacket = new DatagramPacket(
							replyByteArray, replyByteArray.length,
							receivedPacket.getAddress(), listenPort);
					replySocket.send(replyPacket);
					logger.info(
							"Sending LIVE_HOSTS_REPLY length: {} to {} with live hosts: {}",
							serverSocket.getInetAddress(), liveHosts,
							replyByteArray.length);
					break;
				case GOSSIP:
					/**
					 * Extract the Host Table from Gossip message and update its
					 * own Host Table
					 */
					logger.debug("Received GOSSIP from host: {}",
							receivedPacket.getAddress());
					List<HostRecord> hostRecords = inmsg.getHostRecordList();
					gm.updateHostEntries(hostRecords);
					break;
				case GROUP_LEAVE:
					if (inmsg.getHostRecordList().size() > 0) {
						String hostname = inmsg.getHostRecord(0).getHostname();
						logger.info(
								"Received GROUP_LEAVE message from host: {}",
								hostname);
						gm.leaveTheGroup(hostname);
					} else {
						logger.info("Invalid message");
					}
					break;
				default:
					logger.info("Unknown message type: {}", inmsg);
					break;
				}
				// newConnection
			} catch (Exception e) {
				logger.error("Error while listening for Gossip Messages", e);
			}
		}

	}

	public List<String> getActiveLiveHosts() {
		return gm.getActiveHosts();
	}
	
	public boolean isAlive(String hostname){
		return gm.isAlive(hostname);
	}

	public void registerMembershipChangeListener(
			MembershipChangeListener listener) {
		gm.registerMembershipChangeListener(listener);
	}

	private void printBWReceived() {
		logger.info("Bytes received: {} bytes", received);
		received = 0;
	}

	public static void main(String args[]) {
		String configFile = System.getProperty("app.config");
		/**
		 * Read the config file to extract the FailureMonitor specific
		 * parameters from config file
		 */
		config = new HeliumConfig(configFile);
		try {
			config.init();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("ERROR : CANNOT INITIALIZE CONFIG FILE");
		}
		/**
		 * Start the FailureMonitor thread
		 */
		Thread fmThread = new Thread(fm);
		fmThread.start();
	}

	class BWMonitor extends TimerTask {

		@Override
		public void run() {
			printBWReceived();
		}

	}

	public void leave() {
		gm.leave();
	}

}
