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

import java.io.File;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.thrift.transport.TTransportException;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.storage.BerkeleyStorage;
import ch.usi.da.smr.transport.ABSender;
import ch.usi.da.smr.transport.Receiver;
import ch.usi.da.smr.transport.Response;
import ch.usi.da.smr.transport.UDPListener;

/**
 * Name: TestClient<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class TestClient implements Runnable, Receiver {

	private final CountDownLatch barrier;
	
	private final ABSender ab;
	
	private final UDPListener udp;
	
	private Map<Integer,Response> responses = new HashMap<Integer,Response>();
	
	private final InetAddress ip;
	
	private final int port;
	
	public TestClient(String abhost,int abport,CountDownLatch barrier) throws SocketException, TTransportException {
		this.barrier = barrier;
		ip = Client.getHostAddress(false);
		port = 3000 + new Random().nextInt(1000);
		udp = new UDPListener(port);
		ab = new ABSender(abhost,abport);
		Thread t = new Thread(udp);
		t.start();
	}

	@Override
	public void run() {
		udp.registerReceiver(this);
		int i = 1;
		int id;
		String sender = ip.getHostAddress() + ":" + port;
		List<Command> cmds = new ArrayList<Command>();
		cmds.add(new Command(i,CommandType.PUT,"x",("" + port).getBytes()));
		cmds.add(new Command(i,CommandType.PUT,"y",("" + port).getBytes()));
		cmds.add(new Command(i,CommandType.PUT,"z",("" + port).getBytes()));
		while(i < 100000){
			if(i % 1000 == 0){
				System.out.println("Thread " + Thread.currentThread().getId() + " " + i);
			}
			id = (i * 100) + (int)Thread.currentThread().getId();
    		Response r = new Response();
    		responses.put(id,r);
    		Message m = new Message(id,sender,cmds);
	    	int ret = ab.abroadcast(m);
	    	if(ret > 0){
				try {
					r.getResponse(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	    	}else{
	    		System.err.println("Could not send command: " + cmds);
	    		responses.remove(id);
	    	}
	    	i++;
		}
		System.out.println("------------------------");
		cmds.clear();
		id = (int)Thread.currentThread().getId();
		cmds.add(new Command(id,CommandType.GET,"x",new byte[0]));
		cmds.add(new Command(id,CommandType.GET,"y",new byte[0]));
		cmds.add(new Command(id,CommandType.GET,"z",new byte[0]));
		Response r = new Response();
		responses.put(id,r);
		Message m = new Message(id,sender,cmds);
    	int ret = ab.abroadcast(m);
    	if(ret > 0){
			try {
				Message response = r.getResponse(5000);
	    		if(response != null){
	    			for(Command c : response.getCommands()){
		    			if(c.getType() == CommandType.RESPONSE){
		    				System.out.println("Thread " + Thread.currentThread().getId() + "  -> " + new String(c.getValue()));
		    			}			    				
	    			}
	    		}
			} catch (InterruptedException e) {
			}
    	}
    	barrier.countDown();
	}
	
	@Override
	public void receive(Message m) {
		if(responses.containsKey(m.getID())){
			Response r = responses.get(m.getID());
			r.setResponse(m);
			responses.remove(m.getID());
		}		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String zoo_host = "127.0.0.1:2181";
			// start replicas
			Replica r1 = null; //TODO: new Replica(zoo_host,1,new File("/tmp/replica-db/1"));
			r1.start();
			Replica r2 = null; //TODO: new Replica(zoo_host,2,new File("/tmp/replica-db/2"));
			r2.start();
			Replica r3 = null; //TODO: new Replica(zoo_host,3,new File("/tmp/replica-db/3"));
			r3.start();

			// start clients
			CountDownLatch barrier = new CountDownLatch(2);
			Thread t1 = new Thread(new TestClient(zoo_host,2,barrier));
			Thread t2 = new Thread(new TestClient(zoo_host,3,barrier));
			t1.start();
			t2.start();
			barrier.await();
			Thread.sleep(1000);
			System.out.println("------------------------");
			
			// stop replicas (close DB)
			r1.close();
			r2.close();
			r3.close();

			// open DB's
			BerkeleyStorage db1 = new BerkeleyStorage(new File("/tmp/replica-db/1"),true);
			BerkeleyStorage db2 = new BerkeleyStorage(new File("/tmp/replica-db/2"),true);
			BerkeleyStorage db3 = new BerkeleyStorage(new File("/tmp/replica-db/3"),true);

			db1.listAll();
			db2.listAll();
			db3.listAll();

			db1.close();
			db2.close();
			db3.close();

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
