package edu.uiuc.helium.dgrep;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.helium.proto.MessageProtos.GossipMessage;
import edu.uiuc.helium.proto.MessageProtos.GrepMessage;
import edu.uiuc.helium.proto.MessageProtos.GrepMessage.Builder;

/**
 * A set of Utility methods for sending/receiving messages
 * 
 */
public class SocketUtils {

	private static final Logger logger = LoggerFactory
			.getLogger(SocketUtils.class);

	/**
	 * Sends inMessage to hostname at port. Then waits for reply from hostname
	 * and returns the reply back
	 * 
	 * @param hostname
	 *            Host Ip Address to send the message to
	 * @param port
	 *            port to make connection
	 * @param inMessage
	 *            message to send to hostname
	 * @return Reply received from hostname
	 * @throws Exception
	 */
	public static GrepMessage sendAndReceive(String hostname, int port,
			GrepMessage inMessage) throws Exception {
		Socket socket = new Socket();
		try {
			socket.connect(new InetSocketAddress(hostname, port), 2000);
		} catch (SocketTimeoutException e) {
			logger.info("GrepServer at host: {} port: {} is not running",
					hostname, port);
			return null;
		}

		BufferedOutputStream outputStream = new BufferedOutputStream(
				socket.getOutputStream());
		BufferedInputStream inputStream = new BufferedInputStream(
				socket.getInputStream());
		outputStream.write(inMessage.toByteArray());
		outputStream.flush();
		socket.shutdownOutput();
		Builder replyBuilder = GrepMessage.newBuilder().mergeFrom(inputStream);
		GrepMessage reply = replyBuilder.build();
		socket.close();
		return reply;
	}

	/**
	 * Sends inMessage to hostname at port. Then waits for reply from hostname
	 * and returns the reply back
	 * 
	 * @param hostname
	 *            Host Ip Address to send the message to
	 * @param port
	 *            port to make connection
	 * @param inMessage
	 *            message to send to hostname
	 * @return Reply received from hostname
	 * @throws Exception
	 */
	public static GossipMessage sendAndReceive(String hostname, int port,
			GossipMessage inMessage) throws Exception {
		Socket socket = new Socket(hostname, port);
		BufferedOutputStream outputStream = new BufferedOutputStream(
				socket.getOutputStream());
		BufferedInputStream inputStream = new BufferedInputStream(
				socket.getInputStream());
		outputStream.write(inMessage.toByteArray());
		outputStream.flush();
		socket.shutdownOutput();
		GossipMessage reply = GossipMessage.newBuilder().mergeFrom(inputStream)
				.build();
		socket.close();
		return reply;
	}

	public static String getLocalIpAddress() {
		String selfIp = null;
		try {
			Enumeration<NetworkInterface> interfaces = NetworkInterface
					.getNetworkInterfaces();
			while (interfaces.hasMoreElements()) {
				NetworkInterface current = interfaces.nextElement();
				if (!current.isUp() || current.isLoopback()
						|| current.isVirtual())
					continue;
				Enumeration<InetAddress> addresses = current.getInetAddresses();
				while (addresses.hasMoreElements()) {
					InetAddress current_addr = addresses.nextElement();
					if (current_addr.isLoopbackAddress()
							|| !(current_addr instanceof Inet4Address))
						continue;
					selfIp = current_addr.getHostAddress();
				}
			}
		} catch (SocketException e) {
			logger.error("Error fetching the list of Network Interfaces", e);
		}
		return selfIp;
	}

}
