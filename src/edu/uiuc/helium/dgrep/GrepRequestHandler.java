package edu.uiuc.helium.dgrep;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.helium.proto.MessageProtos.GrepMessage;
import edu.uiuc.helium.proto.MessageProtos.GrepMessage.Builder;
import edu.uiuc.helium.proto.MessageProtos.GrepMessage.MessageType;

/**
 * 
 * GrepRequestHandler can be run as a Thread. 
 * GrepServer will receive the Grep Requests and 
 * for each request create a GrepRequestHandler thread.
 * GrepRequestHandler knows how to process 
 * a grep request.
 */
public class GrepRequestHandler implements Runnable {
    private static final Logger logger = LoggerFactory
	    .getLogger(GrepRequestHandler.class);

    private Socket socket;
    private File logFile;

    GrepRequestHandler(Socket s, File logFile) {
	this.socket = s;
	this.logFile = logFile;
    }

    public void run() {
	try {
	    logger.info("Starting processing GrepRequest from {}",
		    socket.getInetAddress());
	    /**
	     * Create streams for reading to and from socket
	     */
	    BufferedInputStream inputStream = new BufferedInputStream(
		    socket.getInputStream());
	    BufferedOutputStream outputStream = new BufferedOutputStream(
		    socket.getOutputStream());
	    /**
	     * Reads the GrepMessage from the InputStream of 
	     * socket.
	     */
	    GrepMessage request = GrepMessage.newBuilder()
		    .mergeFrom(inputStream).build();

	    switch (request.getType()) {
	    case GREP_REQUEST:
		GrepMessage reply = handleGrepRequest(request);
		outputStream.write(reply.toByteArray());
		outputStream.flush();
		socket.close();
		logger.info("Sent GrepReply to {}", socket.getInetAddress());
		break;
	    default:
		logger.info("Unknown request: {}", request);
	    }
	    socket.close();
	} catch (IOException e) {
	    logger.error("Error occurred while processing GrepRequest from {}",
		    socket.getInetAddress(), e);
	}

    }

    private GrepMessage handleGrepRequest(GrepMessage request) {

	/**
	 * Create a Grep class object and pass
	 * the pattern to search to grep class
	 */
	Grep grep = new Grep(logFile);
	List<String> results = grep.search(request.getCommand());

	/**
	 * Create a reply message for the matched 
	 * lines
	 */
	Builder replyBuilder = GrepMessage.newBuilder();
	replyBuilder.setType(MessageType.GREP_REPLY);
	for (String result : results) {
	    replyBuilder.addMatchedLines(result);
	}
	GrepMessage replyMessage = replyBuilder
		.setSenderAddress(SocketUtils.getLocalIpAddress())
		.setLogFileName(logFile.getName()).build();
	return replyMessage;
    }
}
