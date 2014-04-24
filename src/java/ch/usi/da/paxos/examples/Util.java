package ch.usi.da.paxos.examples;
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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.ring.RingDescription;

public class Util {
	
	/**
	 * Parse argument describing the ring into a List<RingDescription>. 
	 * The argument has the format:
	 * ringid,id:PAL (P/A/L are the roles in the ring). 
	 * 
	 * More than one ring can be specified:
	 * ring1id,id1:PAL;ring2id,id2:PAL
	 *  
	 * @param ringsArg
	 * @return list of ring descriptions that can be used to initialize a Node
	 */
	public static List<RingDescription> parseRingsArgument(String ringsArg) {
		// process rings
		List<RingDescription> rings = new ArrayList<RingDescription>();
		for (String r : ringsArg.split(";")) {
			int ringID = Integer
					.parseInt(r.split(":")[0].split(",")[0]);
			int nodeID = Integer
					.parseInt(r.split(":")[0].split(",")[1]);
			String roles = r.split(":")[1];
			rings.add(new RingDescription(ringID, nodeID, getPaxosRoles(roles)));
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
	 * @param ipv6 include IPv6 addresses in search
	 * @return return the host IP address or null
	 */
	public static InetAddress getHostAddress(boolean ipv6){
		try {
			Enumeration<NetworkInterface> ni = NetworkInterface.getNetworkInterfaces();
			while (ni.hasMoreElements()){
				NetworkInterface n = ni.nextElement();
				if(n.getDisplayName().equals("eth0") || n.getDisplayName().equals("p8p2")){
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

}
