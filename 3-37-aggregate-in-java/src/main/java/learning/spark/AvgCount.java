package learning.spark;

import java.io.Serializable;

/**
 * The Class AvgCount.
 */
public class AvgCount implements Serializable {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -1841140741290747204L;

	/** The total. */
	public int total;
	
	/** The num. */
	public int num;
	
	/**
	 * Instantiates a new avg count.
	 *
	 * @param total the total
	 * @param num the num
	 */
	public AvgCount(final int total, final int num) {
		this.total = total;
		this.num = num;
	}
	
	/**
	 * Avg.
	 *
	 * @return the double
	 */
	public double avg() {
		return total / (double) num;
		
	}
	
	
}
