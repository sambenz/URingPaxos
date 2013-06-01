package ch.usi.da.paxos.thrift;
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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.ring.RingDescription;

/**
 * Name: ThriftNode<br>
 * Description: <br>
 * 
 * Creation date: Mar 19, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class ThriftNode {
							
	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) {
		String zoo_host = "127.0.0.1:2181";		
		if(args.length > 1){
			zoo_host = args[1];
		}
		
		if(args.length < 1){
			System.err.println("Plese use \"ThriftNode\" \"ring ID,node ID:roles[;ring,ID:roles]\" (eg. 1,1:PAL)");
		}else{
			try {
				// process rings
				List<RingDescription> rings = new ArrayList<RingDescription>();
				for(String r : args[0].split(";")){
					int ringID = Integer.parseInt(r.split(":")[0].split(",")[0]);
					int nodeID = Integer.parseInt(r.split(":")[0].split(",")[1]);
					String roles = r.split(":")[1];
					rings.add(new RingDescription(ringID,nodeID,Node.getPaxosRoles(roles)));
				}
				// start node in thrift mode
				final Node node = new Node(zoo_host,rings,true);
				Runtime.getRuntime().addShutdownHook(new Thread(){
	                @Override
	                public void run(){
	                	try {
							node.stop();
						} catch (InterruptedException e) {
						}
	                }
	            });
				node.start();
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
	}

}
