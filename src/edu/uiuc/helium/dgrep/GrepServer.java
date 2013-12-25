package edu.uiuc.helium.dgrep;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Grep Server is a Multi Threaded program to handle Grep requests. 
 * It 
 * a) listens on a port for Grep requests from other nodes which are part of the
 * Distributed System 
 * b) Processes the grep request and sends the result to
 * other hosts
 * 
 * @author Manoj
 * 
 */
public class GrepServer implements Runnable {
    private static final Logger logger = LoggerFactory
	    .getLogger(GrepServer.class);
    private int listenPort;

    private ServerSocket socket;
    private int poolsize;
    private ExecutorService newFixedThreadPool;
    private File logFile;

    /**
     * 
     * @param listenPort
     *            the port where GrepServer will listen for GrepRequest
     * @param poolsize
     *            The size of ThreadPool to process GrepRequests.This no of
     *            GrepRequests can be processed concurrantly
     * @param logFile
     *            Absolute path of Log File which GrepSever will look for
     *            results locally
     */
    public GrepServer(int listenPort, int poolsize, String logFile) {
	logger.info("Initializing GrepServer");
	this.listenPort = listenPort;
	this.poolsize = poolsize;
	this.logFile = new File(logFile);
	newFixedThreadPool = Executors.newFixedThreadPool(poolsize);
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
		logger.info("Waiting for new GrepRequests");
		Socket connection = socket.accept();
		/**
		 * For each incoming request, create a GrepRequestHandler 
		 * task and submit the task to ThreadPool for execution
		 */
		GrepRequestHandler clientRequestHandler = new GrepRequestHandler(
			connection, logFile);
		logger.info("Submitting GrepRequest from {} to ThreadPool",
			connection.getInetAddress());
		newFixedThreadPool.submit(clientRequestHandler);
	    } catch (IOException e) {
		logger.error(
			"Error occured with listening for new GrepRequests", e);
	    }
	}
    }

    public static void main(String[] args) {
	/**
	 * Read the config file and read required variables
	 */
	String configFileName = System.getProperty("app.config");
	GrepConfig config = new GrepConfig(configFileName);
	try {
	    config.init();
	} catch (Exception e) {
	    logger.error("Error initializing config for GrepServer", e);
	    System.exit(-1);
	}
	String localHostIp = SocketUtils.getLocalIpAddress();
	String logFilePath = config.getLogFilePath(localHostIp);
	logger.info("Will be using Host: {} logfile: {} for grepping",
		localHostIp, logFilePath);
	if (logFilePath == null) {
	    logger.info("Could not resolve log file path for local host: {}",
		    localHostIp);
	    System.exit(-1);
	}

	/**
	 * Having read the config file,Start the Grep Server 
	 * Thead now
	 */
	GrepServer gs = new GrepServer(config.getGrepServerPort(),
		config.getThreadPoolSize(), logFilePath);
	logger.info("Starting new thread for GrepServer");
	Thread grepServerThread = new Thread(gs);
	grepServerThread.run();
	logger.info("Exiting now");
	return;
    }

}
