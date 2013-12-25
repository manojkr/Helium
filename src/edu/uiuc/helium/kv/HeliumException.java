package edu.uiuc.helium.kv;

/**
 * Special Exception to indicate Key Value based errors
 * 
 * @author Manoj
 * 
 */
public class HeliumException extends Exception {
	public HeliumException() {

	}

	public HeliumException(String msg) {
		super(msg);
	}
}
