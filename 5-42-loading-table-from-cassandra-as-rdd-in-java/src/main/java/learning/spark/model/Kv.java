package learning.spark.model;

import java.io.Serializable;

public class Kv implements Serializable {

	private static final long serialVersionUID = -8059872934792188718L;

	private String key;
	private int value;
	
	public Kv(String key, int value) {
		this.key = key;
		this.value = value;
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(final String key) {
		this.key = key;
	}
	public int getValue() {
		return value;
	}
	public void setValue(final int value) {
		this.value = value;
	}
	
}
