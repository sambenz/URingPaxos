package ch.usi.da.paxos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.storage.Decision;

/**
 * Name: Majority<br>
 * Description: <br>
 * 
 * Creation date: Apr 3, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class Majority {

	private final List<Message> messages = new ArrayList<Message>();
	
	private final Map<Decision,AtomicInteger> values = new HashMap<Decision,AtomicInteger>();
	
	private Decision decision = null;
	
	private int max_ballot = 0;
	
	/**
	 * Empty constructor
	 */
	public Majority(){
	}
	
	/**
	 * Add a message to decide
	 * 
	 * @param m
	 */
	public synchronized void addMessage(Message m){
		messages.add(m);
		if(m.getValue() != null){
			Decision d = new Decision(m.getInstance(),m.getBallot(),m.getValue());
			if(!values.containsKey(d)){
				values.put(d,new AtomicInteger(1));
				max_ballot = m.getBallot();
			}else{
				// store highest ballot
				if(m.getBallot() > max_ballot){
					max_ballot = m.getBallot();
					AtomicInteger i = values.get(d);
					i.incrementAndGet();
					values.put(d,i);
				}else{
					values.get(d).incrementAndGet();
				}
			}
			// majority of same value
			for(Entry<Decision,AtomicInteger> entry : values.entrySet()){
				if(entry.getValue().get() >= Configuration.getQuorum()){
					decision = entry.getKey();
					break;
				}
			}
		}
	}
	
	/**
	 * Is quorum
	 * 
	 * @return true if quorum
	 */
	public synchronized boolean isQuorum(){
		if(values.isEmpty()){
			return (messages.size() >= Configuration.getQuorum());
		}else{
			if(decision == null){
				return false;
			}else{
				return true;
			}
		}
	}
	
	/**
	 * Get the value within the majority of the messages
	 * 
	 * @return the majority value or null if not decided
	 */
	public synchronized Decision getMajorityDecision(){
		return decision;
	}
}
