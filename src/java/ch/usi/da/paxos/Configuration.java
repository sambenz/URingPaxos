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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import ch.usi.da.paxos.api.PaxosRole;

/**
 * Name: Configuration<br>
 * Description: <br>
 * 
 * Creation date: Apr 1, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Configuration {
	/**
	 * the group member addresses
	 */
	public static Map<PaxosRole,SocketAddress> groups = new HashMap<PaxosRole,SocketAddress>();
	
	
	private static Properties conf = null;
	
	/**
	 * Number of acceptors in the system
	 */
	public static int acceptor_count = 1;
	
	/**
	 * Multicast interface
	 */
	public static String multicast_interface = "eth0";
	
	/**
	 * Zookeeper
	 */
	public static String zookeeper = "127.0.0.1:2181";
		
	private Configuration(){	
	}
	
	/**
	 * Get the quorum count you have to reach in this system
	 * 
	 * @return quorum
	 */
	public static int getQuorum(){
		return (int) Math.ceil((double)(acceptor_count+1)/2);
	}
	
	/**
	 * Get all configurations
	 * 
	 * @return all group addresses
	 */
	public static Map<PaxosRole,SocketAddress> getConfiguration(){
		return groups;
	}
	
	/**
	 * Get group address
	 * 
	 * @param group name of the group
	 * @return the group address
	 */
	public static InetSocketAddress getGroup(PaxosRole group){
		return (InetSocketAddress)groups.get(group);
	}
	
	/**
	 * Parse the file
	 * 
	 * @param file
	 * @throws IOException
	 */
	public static void read(String file) throws IOException{
		BufferedReader input = new BufferedReader(new FileReader(file));
		try {
			String line = null;
			while ((line = input.readLine()) != null) {
				if(!line.startsWith("#")){
					String[] token = line.split("\\s+");
					InetAddress ip = InetAddress.getByName(token[1]);
					int port = Integer.parseInt(token[2]);
					SocketAddress address = new InetSocketAddress(ip,port);
					if(token[0].toLowerCase().contains("accept")){
						groups.put(PaxosRole.Acceptor,address);
					}else if(token[0].toLowerCase().contains("propose")){
						groups.put(PaxosRole.Proposer,address);
					}else if(token[0].toLowerCase().contains("learn")){
						groups.put(PaxosRole.Learner,address);
					}else if(token[0].toLowerCase().contains("lead")){
						groups.put(PaxosRole.Leader,address);
					}
				}
			}
		} finally {
			input.close();
		}
		if(conf == null){
			conf = Configuration.getProperties();
		}
	}
	
	/**
	 * @return the multicast interface
	 */
	public static String getInterface(){
		if(conf == null){
			conf = Configuration.getProperties();
		}
		return multicast_interface;
	}
	
	private static Properties getProperties() {
        Properties conf = new Properties();
        InputStream in = Configuration.class.getClassLoader().getResourceAsStream("paxos.properties");
        if (in != null) {
        	try {
				conf.load(in);
				if(conf.containsKey("acceptor_count")){
					acceptor_count = Integer.parseInt(conf.getProperty("acceptor_count"));
				}
				if(conf.containsKey("multicast_interface")){
					multicast_interface = conf.getProperty("multicast_interface");
				}
				if(conf.containsKey("zookeeper")){
					zookeeper = conf.getProperty("zookeeper");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        return conf;
    }
}
