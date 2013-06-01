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
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.transport.TTransportException;

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

	private final UDPSender udp;
	
	private final ABListener ab;
	
	private final BerkeleyStorage db;
	
	public Replica(String abhost,int abport) throws SocketException, TTransportException {
		udp = new UDPSender();
		ab = new ABListener(abhost,abport);
		db = new  BerkeleyStorage();
	}

	public Replica(String abhost,int abport,File dbfile) throws SocketException, TTransportException {
		udp = new UDPSender();
		ab = new ABListener(abhost,abport);
		db = new  BerkeleyStorage(dbfile,false);
	}

	public void start(){
		ab.registerReceiver(this);
		Thread t = new Thread(ab);
		t.start();
		Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
            	db.close();
				ab.close();
            }
        });
	}

	public void close(){
		db.close();
		ab.close();
	}
	
	@Override
	public void receive(Message m) {
		List<Command> cmds = new ArrayList<Command>();
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
		String host = "localhost";
		int port = 9091;
		if(args.length > 0){
			String[] s = args[0].split(":");
			host = s[0];
			port = Integer.parseInt(s[1]);
		}
		try {
			Replica client = new Replica(host,port);
			client.start();
		} catch (SocketException | TTransportException e) {
			e.printStackTrace();
		}
	}

}
