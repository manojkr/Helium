package edu.uiuc.helium.kv;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.helium.proto.MessageProtos.KVMessage;

/**
 * Common functionality is extracted in this class
 * 
 * @author Manoj
 * 
 */
public class HeliumSocketUtils {
	private static final Logger logger = LoggerFactory
			.getLogger(HeliumSocketUtils.class);

	/**
	 * Sends and receive a KVMessage to host kvserver at port kvServerListenPort
	 * 
	 * @param kvmessage
	 * @param kvserver
	 * @param kvServerListenPort
	 * @return
	 */
	public static KVMessage sendAndReceiveKVMessage(KVMessage kvmessage,
			String kvserver, int kvServerListenPort) {
		Socket socket = new Socket();
		try {
			/**
			 * Open a connection to remote server
			 */
			logger.info("Starting connection to {}: {}", kvserver,
					kvServerListenPort);
			socket.connect(new InetSocketAddress(kvserver, kvServerListenPort),
					5000);
			logger.info("Waiting for connection to {}: {}", kvserver,
					kvServerListenPort);
			BufferedOutputStream outputStream = new BufferedOutputStream(
					socket.getOutputStream());
			BufferedInputStream inputStream = new BufferedInputStream(
					socket.getInputStream());
			/**
			 * Send the KVMessages
			 */
			outputStream.write(kvmessage.toByteArray());
			outputStream.flush();
			socket.shutdownOutput();
			/**
			 * Wait for the reply
			 */
			KVMessage reply = KVMessage.newBuilder().mergeFrom(inputStream)
					.build();
			socket.close();
			return reply;
		} catch (Exception e) {
			logger.error("KVServer at host: {} port: {} is not running",
					kvserver, kvServerListenPort);
			return null;
		}

	}

}
