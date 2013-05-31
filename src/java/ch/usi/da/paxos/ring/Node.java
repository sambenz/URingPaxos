package ch.usi.da.paxos.ring;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.thrift.ThriftLearner;
import ch.usi.da.paxos.thrift.ThriftProposer;

/**
 * Name: Node<br>
 * Description: <br>
 * 
 * Creation date: Aug 12, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class Node {
	
	static {
		// get hostname and pid for log file name
		String host = "localhost";
		try {
			Process proc = Runtime.getRuntime().exec("hostname");
			BufferedInputStream in = new BufferedInputStream(proc.getInputStream());
			byte [] b = new byte[in.available()];
			in.read(b);
			in.close();
			host = new String(b).replace("\n","");
		} catch (IOException e) {
		}
		int pid = 0;
		try {
			pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
		} catch (NumberFormatException | IOException e) {
		}
		System.setProperty("logfilename", host + "-" + pid + ".log");
		System.setProperty("valuesfilename", host + "-" + pid + ".values");
	}

	private final Logger logger;
	
	private final List<ZooKeeper> zoos = new ArrayList<ZooKeeper>(); // hold refs to close
	
	private final String zoo_host;
	
	private final List<RingDescription> rings;
	
	private final boolean thrift_service;

	/**
	 * @param zoo_host
	 * @param rings
	 */
	public Node(String zoo_host,List<RingDescription> rings,boolean thrift_service) {
		this.logger = Logger.getLogger(Node.class);
		this.zoo_host = zoo_host;
		this.rings = rings;
		this.thrift_service = thrift_service;
	}

	public void start() throws IOException, KeeperException, InterruptedException{
		try {
			int pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
			logger.info("PID: " + pid);
		} catch (NumberFormatException | IOException e) {
		}
		// node address
		final InetAddress ip = Node.getHostAddress(false);
		boolean start_multi_learner = isMultiLearner(rings);
		for(RingDescription ring : rings){
			// ring socket port
			Random rand = new Random();
			int port = 2000 + rand.nextInt(1000); // assign port between 2000-3000
			InetSocketAddress addr = new InetSocketAddress(ip,port);
			// create ring manager
			ZooKeeper zoo = new ZooKeeper(zoo_host,3000,null);
			zoos.add(zoo);
			RingManager rm = new RingManager(ring.getRingID(),ring.getNodeID(),addr,zoo,"/ringpaxos");
			ring.setRingManager(rm);
			rm.init();
			// register and start roles
			for(PaxosRole role : ring.getRoles()){
				if(!role.equals(PaxosRole.Learner) || !start_multi_learner){
					if(role.equals(PaxosRole.Proposer)){
						if(thrift_service){
							ThriftProposer p = new ThriftProposer(rm);
							Thread t = new Thread(p);
							t.setName("ThriftProposer");
							t.start();
						}else{
							Role r = new ProposerRole(rm);
							logger.debug("Node register role: " + role + " at node " + ring.getNodeID() + " in ring " + ring.getRingID());
							rm.registerRole(role);		
							Thread t = new Thread(r);
							t.setName(role.toString());
							t.start();
						}
					}else if(role.equals(PaxosRole.Acceptor)){
						Role r = new AcceptorRole(rm);
						logger.debug("Node register role: " + role + " at node " + ring.getNodeID() + " in ring " + ring.getRingID());
						rm.registerRole(role);		
						Thread t = new Thread(r);
						t.setName(role.toString());
						t.start();						
					}else if(role.equals(PaxosRole.Learner)){
						if(thrift_service){
							ThriftLearner l = new ThriftLearner(rm);
							Thread t = new Thread(l);
							t.setName("ThriftLearner");
							t.start();
						}else{
							Role r = new LearnerRole(rm);
							logger.debug("Node register role: " + role + " at node " + ring.getNodeID() + " in ring " + ring.getRingID());
							rm.registerRole(role);		
							Thread t = new Thread(r);
							t.setName(role.toString());
							t.start();
						}
					}
				}
			}			
		}
		if(start_multi_learner){ // start only one super learner
			if(thrift_service){
				logger.error("Node thrift MultiRingLearner not implemented!");
			}else{
				logger.debug("starting a MultiRingLearner");
				Thread t = new Thread(new MultiLearnerRole(rings));
				t.setName("MultiRingLearner");
				t.start();
			}
		}	
	}
	
	public void stop() throws InterruptedException{
		for(RingDescription r : rings){
			RingManager ring = r.getRingmanger();
			if(ring.getNetwork().getAcceptor() != null){
		    	((AcceptorRole)ring.getNetwork().getAcceptor()).getStableStorage().close();
		    }
        	ring.getNetwork().disconnectClient();
        	ring.getNetwork().closeServer();
		}
		for(ZooKeeper zoo : zoos){
			zoo.close();
		}
	}
	
	/**
	 * @return the all rings
	 */
	public List<RingDescription> getRings(){
		return rings;
	}
						
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
			System.err.println("Plese use \"Node\" \"ring ID,node ID:roles[;ring,ID:roles]\" (eg. 1,1:PAL)");
		}else{
			try {
				// process rings
				List<RingDescription> rings = new ArrayList<RingDescription>();
				for(String r : args[0].split(";")){
					int ringID = Integer.parseInt(r.split(":")[0].split(",")[0]);
					int nodeID = Integer.parseInt(r.split(":")[0].split(",")[1]);
					String roles = r.split(":")[1];
					rings.add(new RingDescription(ringID,nodeID,getPaxosRoles(roles)));
				}
				// start node
				final Node node = new Node(zoo_host,rings,false);
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

	private boolean isMultiLearner(List<RingDescription> rings) {
		int learner_count = 0;
		for(RingDescription ring : rings){
			if(ring.getRoles().contains(PaxosRole.Learner)){
				learner_count++;
			}
		}
		return learner_count > 1 ? true : false;
	}

	/**
	 * Get the host IP address
	 * 
	 * @param ipv6 include IPv6 addresses in search
	 * @return return the host IP address or null
	 */
	public static InetAddress getHostAddress(boolean ipv6){
		try {
			Enumeration<NetworkInterface> ni = NetworkInterface.getNetworkInterfaces();
			while (ni.hasMoreElements()){
				NetworkInterface n = ni.nextElement();
				if(n.getDisplayName().equals("eth0")){
					Enumeration<InetAddress> ia = n.getInetAddresses();
					while(ia.hasMoreElements()){
						InetAddress addr = ia.nextElement();
						if(!(addr.isLinkLocalAddress() || addr.isLoopbackAddress() || addr.toString().contains("192.168.122"))){
							if(addr instanceof Inet6Address && ipv6){
								return addr;
							}else if (addr instanceof Inet4Address && !ipv6){
								return addr;
							}
						}
					}
				}
			}
			return InetAddress.getLoopbackAddress();
		} catch (SocketException e) {
			return InetAddress.getLoopbackAddress();
		}
	}

	/**
	 * Get the list of PaxosRole from String
	 * 
	 * @param rs roles as string
	 * @return return PaxosRole enum list
	 */
	public static List<PaxosRole> getPaxosRoles(String rs){
		List<PaxosRole> roles = new ArrayList<PaxosRole>();
		if(rs.toLowerCase().contains("p")){
			roles.add(PaxosRole.Proposer);
		}
		if(rs.toLowerCase().contains("a")){
			roles.add(PaxosRole.Acceptor);
		}
		if(rs.toLowerCase().contains("l")){
			roles.add(PaxosRole.Learner);
		}
		return roles;	
	}	
}
