package ch.usi.da.paxos.thrift;

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
 * @author benz@geoid.ch
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
