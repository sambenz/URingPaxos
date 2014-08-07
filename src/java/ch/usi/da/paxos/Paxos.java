package ch.usi.da.paxos;
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

import java.net.DatagramPacket;
import java.util.ArrayList;
import java.util.List;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.storage.Decision;

/**
 * Name: Paxos<br>
 * Description: <br>
 * 
 * Creation date: Apr 1, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Paxos {

	private Value value = null;
	
	private int ballot = 0;
	
	private final Acceptor acceptor;
	
	private final long instance;
	
	/**
	 * Public constructor
	 * 
	 * @param acceptor a acceptor instance
	 * @param instance the instance of paxos
	 */
	public Paxos(Acceptor acceptor,Long instance){
		this.acceptor = acceptor;
		this.instance = instance;
		if(acceptor.getHistory().containsKey(instance)){
			Decision d = acceptor.getHistory().get(instance);
			ballot = d.getBallot();
			value = d.getValue();
		}
	}
	
	/**
	 * Return the value
	 * 
	 * @return the value
	 */
	public Value getValue(){
		return value;
	}
	
	/**
	 * Get the instance
	 * 
	 * @return paxos instance 
	 */
	public long getInstance(){
		return instance;
	}
	
	/**
	 * Process a packet within a paxos instance
	 * 
	 * @param p
	 * @return the answer packet
	 */
	public List<DatagramPacket> process(DatagramPacket p) {
		List<DatagramPacket> out = new ArrayList<DatagramPacket>();
		if(p != null){
			Message request = Message.fromWire(p.getData());
				
			// 1a
			if(request.getType() == MessageType.Prepare){
				if(request.getBallot() > ballot){ // 1b
					ballot = request.getBallot();
					Message m = new Message(instance,acceptor.getID(),PaxosRole.Proposer,MessageType.Promise,ballot,value);
					// WARNING: do not send packets directly to the address! NIO channel don't like this! 
					//byte[] b = Message.toWire(m);
					//DatagramPacket packet = new DatagramPacket(b,b.length,p.getAddress(),p.getPort());
					//out.add(packet);
					send(out,m);
				}else{ // the NACK message
					Message m = new Message(instance,acceptor.getID(),PaxosRole.Proposer,MessageType.Nack,ballot,value);
					send(out,m);
				}
			}else if (request.getType() == MessageType.Accept){
				if(request.getBallot() >= ballot){ // >= see P1a
					ballot = request.getBallot();
					if(value == null){ // otherwise you can reserve a new ballot and overwrite the old value
						value = request.getValue();
					}
					if(value != null){ // 2b
						acceptor.getHistory().put(instance,new Decision(0,instance,ballot,value));
						Message m1 = new Message(instance,acceptor.getID(),PaxosRole.Proposer,MessageType.Accepted,ballot,value);
						send(out,m1);
						Message m2 = new Message(instance,acceptor.getID(),PaxosRole.Learner,MessageType.Accepted,ballot,value);
						send(out,m2);
						// decided
					}
				}
			}
		}
		if(value != null){
			acceptor.getInstanceList().remove(instance);
		}
		return out;
	}

	private void send(List<DatagramPacket> l,Message m){
		try {
			byte[] b = Message.toWire(m);
			DatagramPacket p = new DatagramPacket(b,b.length,Configuration.getGroup(m.getReceiver()));
			l.add(p);
		} catch (Exception e) {
		}
	}

}
