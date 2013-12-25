package edu.uiuc.helium.gossip;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import asg.cliche.Command;
import asg.cliche.ShellFactory;

public class FailureMonitorCLI {

	private boolean hasJoined = false;
	private static HeliumConfig config;
	private FailureMonitor fm;

	@Command
	public void leave() {
		if (hasJoined) {
			System.out.format("%s Leaving the group\n", new Date());
			fm.leave();
			try {
				TimeUnit.SECONDS.sleep(2);
			} catch (InterruptedException e) {
				System.out.println("Error while waiting");
			}
			System.exit(0);
		} else {
			System.out.println("You have not joined the GROUP yet");
		}
	}

	@Command
	public void join() {
		if (!hasJoined) {
			fm = new FailureMonitor(config);
			Thread fmThread = new Thread(fm);
			fmThread.start();
			System.out.format("%s Joining the group\n", new Date());
			hasJoined = true;
		} else {
			System.out.println("You have already joined the GROUP");
		}
	}

	public static void main(String[] args) throws Exception {
		String configFile = System.getProperty("app.config");
		/**
		 * Read the config file to extract the FailureMonitor specific
		 * parameters from config file
		 */
		config = new HeliumConfig(configFile);
		try {
			config.init();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("ERROR: CANNOT INITIALIZE CONFIG FILE");
		}

		ShellFactory.createConsoleShell("CS425", "", new FailureMonitorCLI())
				.commandLoop();

	}

}
