package ch.usi.da.paxos.api;
/* 
 * Copyright (c) 2013 Università della Svizzera italiana (USI)
 * 
 * This file is part of URingPaxos.
 *
 * URingPaxos is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * URingPaxos is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with URingPaxos.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * Name: ConfigKey<br>
 * Description: <br>
 * 
 * Creation date: Sep 16, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
abstract public class ConfigKey {

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
	public static final String batch_policy = "proposer_batch_policy";

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
	public static final String tcp_crc = "tcp_crc";

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

	/**
	 * see RingManager for default
	 */	
	public static final String multi_ring_start_time = "multi_ring_start_time";

	/**
	 * see RingManager for default
	 */	
	public static final String reference_ring = "reference_ring";
	
	/**
	 * see RingManager for default
	 */	
	public static final String deliver_skip_messages = "deliver_skip_messages";

	/**
	 * see RingManager for default
	 */	
	public static final String trim_quorum = "trim_quorum";

	/**
	 * see RingManager for default
	 */	
	public static final String trim_modulo = "trim_modulo";

	/**
	 * see RingManager for default
	 */	
	public static final String auto_trim = "auto_trim";

}
