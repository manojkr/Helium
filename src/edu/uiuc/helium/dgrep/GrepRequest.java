package edu.uiuc.helium.dgrep;

import java.net.Socket;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;

import edu.uiuc.helium.proto.MessageProtos.GrepMessage;
import edu.uiuc.helium.proto.MessageProtos.GrepMessage.MessageType;

/**
  
 * GrepRequest represents a task corresponding
 * to a single Grep search from client. 
 * 
 * It is responsible for establishing a socket connection
 * with GrepServer, sending the Grep command to GrepServer,
 * and returning the results to user.
 * 
 * @author Manoj
 *
 */
public class GrepRequest implements Callable<GrepMessage> {
    private GrepMessage reply;
    private int port;
    private String hostname;
    private String[] command;

    public GrepRequest(String hostname, int port, String[] command) {
    	this.hostname = hostname;
	this.port = port;
	this.command = command;
    }

    public GrepMessage call() throws Exception {
	/**
	 * Create a GrepMessage with type GREP_REQUEST.
	 * This is a compressed message representation
	 * and will be sent over the network
	 */
	GrepMessage request = GrepMessage.newBuilder()
		.setType(MessageType.GREP_REQUEST)
		.setSenderAddress(SocketUtils.getLocalIpAddress())
		.setCommand(StringUtils.join(command, " ")).build();

	GrepMessage reply = SocketUtils.sendAndReceive(hostname, port, request);
	return reply;
    }

}
