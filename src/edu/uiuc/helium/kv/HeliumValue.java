package edu.uiuc.helium.kv;

import java.io.Serializable;

/**
 * Class to store (Timestamp, Value) pair which will be stored as Value in
 * HelenusServer
 * 
 * @author mkumar11
 * 
 */
public class HeliumValue implements Serializable {

	private static final long serialVersionUID = 1L;
	Object value;
	Long timestamp;

	public HeliumValue(Object value, Long timestamp) {
		this.value = value;
		this.timestamp = timestamp;
	}

	public Object getValue() {
		return value;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "HelenusValue [value=" + value + ", timestamp=" + timestamp
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((timestamp == null) ? 0 : timestamp.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HeliumValue other = (HeliumValue) obj;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

}
