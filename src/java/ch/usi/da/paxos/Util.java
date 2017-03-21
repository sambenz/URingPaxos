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

import java.beans.ExceptionListener;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.ring.RingDescription;

public class Util {

	/**
	 * Parse argument describing the ring into a List of RingDescription . 
	 * The argument has the format:
	 * ringid:PAL (P/A/L are the roles in the ring). 
	 * 
	 * More than one ring can be specified:
	 * ring1id:PAL;ring2id:PAL
	 *  
	 * @param ringsArg String of ring properties to parse
	 * @return list of ring descriptions that can be used to initialize a Node
	 */
	public static List<RingDescription> parseRingsArgument(String ringsArg) {
		// process rings
		List<RingDescription> rings = new ArrayList<RingDescription>();
		for (String r : ringsArg.split(";")) {
			int ringID = Integer.parseInt(r.split(":")[0]);
			String roles = r.split(":")[1];
			rings.add(new RingDescription(ringID, getPaxosRoles(roles)));
		}
		return rings;
	}

	private static List<PaxosRole> getPaxosRoles(String rs) {
		List<PaxosRole> roles = new ArrayList<PaxosRole>();
		if (rs.toLowerCase().contains("p")) {
			roles.add(PaxosRole.Proposer);
		}
		if (rs.toLowerCase().contains("a")) {
			roles.add(PaxosRole.Acceptor);
		}
		if (rs.toLowerCase().contains("l")) {
			roles.add(PaxosRole.Learner);
		}
		return roles;
	}

	/**
	 * Get the host IP address
	 * 
	 * Use env(IFACE) to select an interface or
	 * env(IP) to select a specific address
	 * 
	 * to prefer IPv6 use: java.net.preferIPv6Stack=true
	 * 
	 * @return return the host IP address or 127.0.0.1 (::1)
	 */
	public static InetAddress getHostAddress(){
		boolean ipv6 = false;
		String pv4 = System.getProperty("java.net.preferIPv4Stack");
		String pv6 = System.getProperty("java.net.preferIPv6Stack");
		if(pv4 != null && pv4.equals("false")){
			ipv6 = true;
		}		
		if(pv6 != null && pv6.equals("true")){
			ipv6 = true;
		}
		try {
			String iface = System.getenv("IFACE");			
			String public_ip = System.getenv("IP");
			if(public_ip != null){
				return InetAddress.getByName(public_ip);
			}
			Enumeration<NetworkInterface> ni = NetworkInterface.getNetworkInterfaces();
			while (ni.hasMoreElements()){
				NetworkInterface n = ni.nextElement();
				if(iface == null || n.getDisplayName().equals(iface)){
					Enumeration<InetAddress> ia = n.getInetAddresses();
					while(ia.hasMoreElements()){
						InetAddress addr = ia.nextElement();
						if(!(addr.isLinkLocalAddress() || addr.isLoopbackAddress() || addr.toString().contains("172.17"))){
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
		} catch (SocketException | UnknownHostException e) {
			return InetAddress.getLoopbackAddress();
		}
	}


	/**
	 * This method fixes the race condition faced by the client when checking if
	 * a znode already exists before creating one. It basically ignores the 
	 * <b>NodeExistsException</b>, leaving the existing znode untouched.
	 * 
	 * @param path
	 *                the path for the node
	 * @param data
	 *                the initial data for the node
	 * @param acl
	 *                the acl for the node
	 * @param createMode
	 *                specifying whether the node to be created is ephemeral
	 *                and/or sequential
	 * @param zooClient 
	 *                a ZooKeeper client object
	 * @return the actual path of the created node
	 * @throws KeeperException if the server returns a non-zero error code
	 * @throws KeeperException.InvalidACLException if the ACL is invalid, null, or empty
	 * @throws InterruptedException if the transaction is interrupted
	 * @throws IllegalArgumentException if an invalid path is specified
	 */
	public static String checkThenCreateZooNode(final String path, byte data[], List<ACL> acl, CreateMode createMode,
			ZooKeeper zooClient) throws KeeperException, InterruptedException {
		return checkThenCreateZooNode(path, data, acl, createMode, zooClient, null);
	}


	/**
	 * This method fixes the race condition faced by the client when checking if
	 * a znode already exists before creating one. It basically handles the 
	 * <b>NodeExistsException</b>, if an exception handler was given, or ignore
	 * it, otherwise.
	 * 
	 * @param path
	 *                the path for the node
	 * @param data
	 *                the initial data for the node
	 * @param acl
	 *                the acl for the node
	 * @param createMode
	 *                specifying whether the node to be created is ephemeral
	 *                and/or sequential
	 * @param zooClient 
	 *                a ZooKeeper client object
	 * @param exceptionHandler
	 *                the object that will handle the NodeExistsExcpetion; if this
	 *                is null, the exception is ignored and the execution continues
	 *                without replacing the already existing znode
	 * @return the actual path of the created node
	 * @throws KeeperException if the server returns a non-zero error code
	 * @throws KeeperException.InvalidACLException if the ACL is invalid, null, or empty
	 * @throws InterruptedException if the transaction is interrupted
	 * @throws IllegalArgumentException if an invalid path is specified
	 */
	public static String checkThenCreateZooNode(final String path, byte data[], List<ACL> acl, CreateMode createMode,
			ZooKeeper zooClient, ExceptionListener exceptionHandler) throws KeeperException, InterruptedException {

		String createReturn = null;
		try {
			createReturn = zooClient.create(path, data, acl, createMode);
		} catch (NodeExistsException e) {
			if (exceptionHandler != null){
				exceptionHandler.exceptionThrown(e);
			}
		}
		return createReturn;
	}

	/**
	 * @param value
	 * @return a byte[]
	 */
	public static synchronized final byte[] intToByte(int value) {
	    return new byte[] {
	            (byte)(value >>> 24),
	            (byte)(value >>> 16),
	            (byte)(value >>> 8),
	            (byte)value};
	}
	
	/**
	 * @param b
	 * @return the int
	 */
	public static synchronized final int byteToInt(byte [] b) { 
		return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8) + (b[3] & 0xFF); 
	}

	/**
	 * @param value
	 * @return a byte[]
	 */
	public static synchronized final byte[] longToByte(long value) {
	    return new byte[] {
	            (byte)(value >>> 56),
	            (byte)(value >>> 48),
	            (byte)(value >>> 40),
	            (byte)(value >>> 32),	    		
	            (byte)(value >>> 24),
	            (byte)(value >>> 16),
	            (byte)(value >>> 8),
	            (byte)value};
	}
	
	/**
	 * @param b
	 * @return the int
	 */
	public static synchronized final long byteToLong(byte [] b) { 
		return (b[0] << 56) + ((b[1] & 0xFF) << 48) + ((b[2] & 0xFF) << 40) + ((b[3] & 0xFF) << 32) + ((b[4] & 0xFF) << 24) + ((b[5] & 0xFF) << 16) + ((b[6] & 0xFF) << 8) + (b[7] & 0xFF); 
	}

}
