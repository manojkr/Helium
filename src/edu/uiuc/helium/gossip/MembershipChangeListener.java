package edu.uiuc.helium.gossip;

import java.util.List;


public interface MembershipChangeListener {

	public void nodeJoined(String node, boolean isActive);

	public void nodeLeft(String node, boolean isActive);

	public void nodeCrashed(String node, boolean isActive);
}
