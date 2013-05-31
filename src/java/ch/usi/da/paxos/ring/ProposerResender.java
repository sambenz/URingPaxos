package ch.usi.da.paxos.ring;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.storage.Proposal;

/**
 * Name: ProposerResender<br>
 * Description: <br>
 * 
 * Creation date: Sep 14, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class ProposerResender implements Runnable {
	
	private final static Logger logger = Logger.getLogger(ProposerResender.class);
	
	private final ProposerRole proposer;
	
	private int resend_time = 3000;
	
	/**
	 * @param proposer
	 */
	public ProposerResender(ProposerRole proposer) {
		this.proposer = proposer;
		if(proposer.getRingManager().getConfiguration().containsKey(ConfigKey.value_resend_time)){
			resend_time = Integer.parseInt(proposer.getRingManager().getConfiguration().get(ConfigKey.value_resend_time));
			logger.info("Proposer value_resend_time: " + resend_time);
		}
	}

	@Override
	public void run() {
		while(true){
			try {
				Thread.sleep(200);
				long time = System.currentTimeMillis();
				Iterator<Entry<String, Proposal>> i = proposer.getProposals().entrySet().iterator();
				while(i.hasNext()){
					Entry<String, Proposal> e = i.next();
					if(time-e.getValue().getDate()>resend_time){
						i.remove();
						logger.error("Proposer timeout in proposing value: " + e.getValue().getValue());
						proposer.send(new Message(0,proposer.getRingManager().getNodeID(),PaxosRole.Leader,MessageType.Value,0,e.getValue().getValue()));
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;				
			}
		}
	}

}
