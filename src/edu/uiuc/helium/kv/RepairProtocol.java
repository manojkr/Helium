package edu.uiuc.helium.kv;

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.helium.proto.MessageProtos.KVMessage;

/**
 * Repair protocol to send the messages asynchronously for ONE/QUORUM
 * consistency level
 * 
 * @author mkumar11
 * 
 */
public class RepairProtocol implements Runnable {

	private static final Logger _logger = LoggerFactory
			.getLogger(RepairProtocol.class);
	private LinkedBlockingQueue<AsyncUpdateMessage> messages = new LinkedBlockingQueue<AsyncUpdateMessage>();
	private int listenPort;

	public RepairProtocol(int serverListenPort) {
		this.listenPort = serverListenPort;
	}
	
	public void queueMessage(AsyncUpdateMessage msg){
		_logger.info("Adding message to Queue");
		try {
			messages.put(msg);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		while (true) {
			/**
			 * Wait for the message for the queue
			 */
			try {
				AsyncUpdateMessage msg = messages.take();
				String hostname = msg.getHostname();
				HeliumSocketUtils.sendAndReceiveKVMessage(msg.getMsg(),
						hostname, listenPort);
			} catch (InterruptedException e) {
				_logger.error("Error while waiting", e);
			}
		}
	}

}

/**
 * Class containing KVMessage to be sent and host to which message should be
 * sent
 * 
 */
class AsyncUpdateMessage {
	KVMessage msg;
	String hostname;

	public AsyncUpdateMessage(KVMessage msg, String hostname) {
		super();
		this.msg = msg;
		this.hostname = hostname;
	}

	public KVMessage getMsg() {
		return msg;
	}

	public String getHostname() {
		return hostname;
	}

}
