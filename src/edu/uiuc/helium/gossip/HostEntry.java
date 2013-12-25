package edu.uiuc.helium.gossip;

/**
 * 
 * Information about Live Hosts is maintained via HostEntry object. This
 * encapsulates the required fields that are used by Gossip Protocol
 * 
 * @author Manoj
 * 
 */
class HostEntry {
	private String host;
	private int counter;
	private long lastUpdateTime;
	private boolean isRemovable = false;
	private boolean isActive = true;

	public HostEntry(String host) {
		this.host = host;
		this.lastUpdateTime = System.currentTimeMillis();
		this.counter = 0;
	}

	public HostEntry(String host, int counter) {
		this.host = host;
		this.lastUpdateTime = System.currentTimeMillis();
		this.counter = counter;
	}

	/**
	 * 
	 * @return Ip Address of the Host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * 
	 * @return Current counter for the Host
	 */
	public int getCounter() {
		return counter;
	}

	/**
	 * 
	 * @return Last Time when this host entry was updated
	 */
	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	/**
	 * 
	 * @param counter
	 *            Sets new counter value for this host to counter
	 */
	public void setCounter(int counter) {
		this.counter = counter;
	}

	/**
	 * 
	 * @param newTime
	 *            Set the last update time for this host entry
	 */
	public void updateTime(long newTime) {
		this.lastUpdateTime = newTime;
	}

	/**
	 * Increment the counter for host
	 */
	public void incrementCounter() {
		counter += 1;
	}

	/**
	 * @return Returns true if the entry has been marked as Ready to Delete due
	 *         to tFail elapsed time
	 */
	public boolean isRemovable() {
		return isRemovable;
	}

	/**
	 * Mark the host as failed
	 * 
	 * @param isRemovable
	 */
	public void setRemovable(boolean isRemovable) {
		this.isRemovable = isRemovable;
	}

	public boolean isActive() {
		return isActive;
	}

	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}
	
	

}