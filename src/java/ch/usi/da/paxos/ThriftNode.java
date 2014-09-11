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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;

import ch.usi.da.paxos.api.Learner;
import ch.usi.da.paxos.api.Proposer;
import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.ring.RingDescription;
import ch.usi.da.paxos.thrift.ThriftLearner;
import ch.usi.da.paxos.thrift.ThriftProposer;

/**
 * Name: ThriftNode<br>
 * Description: <br>
 * 
 * Creation date: Mar 19, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ThriftNode {
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
		System.setProperty("proposalfilename", host + "-" + pid + ".proposal");		
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
			System.err.println("Plese use \"ThriftNode\" \"ring ID,node ID:roles[;ring,ID:roles]\" (eg. 1,1:PAL)");
		} else {
			// process rings
			List<RingDescription> rings = Util.parseRingsArgument(args[0]);

			// start paxos node
			final Node node = new Node(zoo_host, rings);
			try {
				node.start();
				Runtime.getRuntime().addShutdownHook(new Thread(){
					@Override
					public void run(){
						try {
							node.stop();
						} catch (InterruptedException e) {
						}
					}
				});
			} catch (IOException | KeeperException | InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}

			// start thrift learner
			Learner l = node.getLearner();
			if (l != null) {
				Thread tl = new Thread(new ThriftLearner(l, rings.get(0).getNodeID() + 9090));
				tl.start();
			}

			// start thrift proposer
			Proposer p = node.getProposer(rings.get(0).getRingID());
			if (p != null) {
				Thread tp = new Thread(new ThriftProposer(p, rings.get(0).getNodeID() + 9080));
				tp.start();
			}
		}
	}

}
