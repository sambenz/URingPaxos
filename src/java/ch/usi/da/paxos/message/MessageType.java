package ch.usi.da.paxos.message;

/**
 * Name: MessageType<br>
 * Description: <br>
 * 
 * Creation date: Mar 31, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public enum MessageType {
	/**
	 * 1a
	 */
	Prepare,
	/**
	 * 1a nack
	 */
	Nack,
	/**
	 * 1b
	 */
	Promise,
	/**
	 * 2a
	 */
	Accept,
	/**
	 * 2b
	 */
	Accepted,
	/**
	 * send a value to a leader
	 */
	Value,
	/**
	 * new type for RingPaxos (combines 1a/1b)
	 */
	Phase1,
	/**
	 * new type for RingPaxos (multi ballot reservation)
	 */
	Phase1Range,
	/**
	 * new type for RingPaxos (combines 2a/2b)
	 */
	Phase2,
	/**
	 * new type for RingPaxos (this msg type will sent to ALL roles)
	 */
	Decision
}
