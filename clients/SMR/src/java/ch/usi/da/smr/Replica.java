package ch.usi.da.smr;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.storage.BerkeleyStorage;
import ch.usi.da.smr.transport.ABListener;
import ch.usi.da.smr.transport.Receiver;
import ch.usi.da.smr.transport.UDPSender;

/**
 * Name: Replica<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class Replica implements Receiver {

	private final static Logger logger = Logger.getLogger(Replica.class);
	
	private final PartitionManager partitions;
	
	private final Partition partition;
	
	private final int replicaID;
	
	private final UDPSender udp;
	
	private final ABListener ab;
	
	private final BerkeleyStorage db;
	
	public Replica(PartitionManager partitions, Partition partition, int replicaID) throws IOException, TTransportException {
		this.partitions = partitions;
		this.partition = partition;
		this.replicaID = replicaID;
		udp = new UDPSender();
		ab = partitions.getABListener(partition,replicaID);
		db = new  BerkeleyStorage();
	}

	public Replica(PartitionManager partitions, Partition partition, int replicaID, File dbfile) throws IOException, TTransportException {
		this.partitions = partitions;
		this.partition = partition;
		this.replicaID = replicaID;
		udp = new UDPSender();
		ab = partitions.getABListener(partition,replicaID);
		db = new  BerkeleyStorage(dbfile,false);
	}

	public void start(){
		ab.registerReceiver(this);
		logger.info("Replica start serving partition: " + partition);
		Thread t = new Thread(ab);
		t.start();
	}

	public void close(){
		db.close();
		ab.close();
		partitions.deregister(partition,replicaID);
	}
	
	@Override
	public void receive(Message m) {
		List<Command> cmds = new ArrayList<Command>();
		//TODO: store to mem
		//      once flushed to disk -> send safe
		//      setState(int instance);
		//      int getState()
		//      if getState()+1 != deliver_queue instance -> getState()
		for(Command c : m.getCommands()){
	    	switch(c.getType()){
	    	case PUT:
	    		if(db.put(c.getKey(),c.getValue())){
	    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"OK".getBytes());
	    			cmds.add(cmd);
	    		}else{
	    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"FAIL".getBytes());
	    			cmds.add(cmd);
	    		}
	    		break;
			case DELETE:
	    		if(db.delete(c.getKey())){
	    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"OK".getBytes());
	    			cmds.add(cmd);
	    		}else{
	    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"FAIL".getBytes());
	    			cmds.add(cmd);
	    		}
				break;
			case GET:
				byte[] data = db.get(c.getKey());
				if(data != null){
	    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),data);
	    			cmds.add(cmd);
				}else{
	    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"<no entry>".getBytes());
	    			cmds.add(cmd);
				}
				break;
			case GETRANGE:
				//TODO: implement GETRANGE
				System.err.println("GETRANGE for partition " + partition + " not implemented!");
				break;
			default:
				System.err.println("Receive RESPONSE as Command!"); break;
	    	}
		}
		Message msg = new Message(m.getID(),m.getSender(),cmds);
		udp.send(msg);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String zoo_host = "127.0.0.1:2181";
		if (args.length > 1) {
			zoo_host = args[1];
		}
		if (args.length < 1) {
			System.err.println("Plese use \"Replica\" \"replicaID,ringID,Token\"");
		} else {
			String[] arg = args[0].split(",");
			final int replicaID = Integer.parseInt(arg[0]);
			final int ringID = Integer.parseInt(arg[1]);
			final int token = Integer.parseInt(arg[2]);
			try {
				final ZooKeeper zoo = new ZooKeeper(zoo_host,3000,null);
				final PartitionManager partitions = new PartitionManager(zoo);
				final Partition partition = new Partition(ringID,0,token);
				partitions.init();
				final Replica replica = new Replica(partitions,partitions.register(partition,replicaID),replicaID);
				Runtime.getRuntime().addShutdownHook(new Thread(){
					@Override
					public void run(){
						replica.close();
						try {
							zoo.close();
						} catch (InterruptedException e) {
						}
					}
				});
				replica.start();
				BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
				in.readLine();
				replica.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

}
