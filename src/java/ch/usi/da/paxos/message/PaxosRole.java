package ch.usi.da.paxos.message;

/**
 * Name: MessageGroup<br>
 * Description: <br>
 * 
 * Creation date: Mar 31, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public enum PaxosRole {
	/**
	 * The proposer
	 */
	Proposer,
	/**
	 * The acceptors
	 */
	Acceptor,
	/**
	 * The learners
	 */
	Learner,
	/**
	 * The leader
	 */
	Leader,
}
