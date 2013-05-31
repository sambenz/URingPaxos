package ch.usi.da.paxos.ring;

/**
 * Name: ConfigKey<br>
 * Description: <br>
 * 
 * Creation date: Sep 16, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
abstract class ConfigKey {

	/**
	 * see RingManager for default
	 */
	public static final String p1_preexecution_number = "p1_preexecution_number";

	/**
	 * see RingManager for default
	 */
	public static final String p1_resend_time = "p1_resend_time";

	/**
	 * see RingManager for default
	 */
	public static final String concurrent_values = "concurrent_values";

	/**
	 * see RingManager for default
	 */
	public static final String value_size = "value_size";

	/**
	 * see RingManager for default
	 */	
	public static final String value_count = "value_count";

	/**
	 * see RingManager for default
	 */
	public static final String value_resend_time = "value_resend_time";
	
	/**
	 * see RingManager for default
	 */
	public static final String quorum_size = "quorum_size";

	/**
	 * see RingManager for default
	 */
	public static final String tcp_nodelay = "tcp_nodelay";

	/**
	 * see RingManager for default
	 */
	public static final String stable_storage = "stable_storage";

	/**
	 * see RingManager for default
	 */
	public static final String learner_recovery = "learner_recovery";

	/**
	 * see RingManager for default
	 */	
	public static final String buffer_size = "buffer_size";
	
	/**
	 * see RingManager for default
	 */	
	public static final String multi_ring_m = "multi_ring_m";

	/**
	 * see RingManager for default
	 */	
	public static final String multi_ring_lambda = "multi_ring_lambda";

	/**
	 * see RingManager for default
	 */	
	public static final String multi_ring_delta_t = "multi_ring_delta_t";
	
}
