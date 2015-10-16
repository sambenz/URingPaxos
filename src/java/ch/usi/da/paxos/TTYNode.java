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
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import ch.usi.da.paxos.api.PaxosNode;
import ch.usi.da.paxos.lab.Experiment1;
import ch.usi.da.paxos.message.Control;
import ch.usi.da.paxos.message.ControlType;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.ring.ProposerRole;
import ch.usi.da.paxos.ring.RingDescription;
import ch.usi.da.paxos.storage.Decision;

/**
 * Name: TTYNode<br>
 * Description: <br>
 * 
 * Creation date: Jun 21, 2013<br>
 * $Id$
 * 
 * @author leandro.pacheco.de.sousa@usi.ch
 */
public class TTYNode {
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
		System.setProperty("logfilename", "L" + host + "-" + pid + ".log");
		System.setProperty("valuesfilename", "L" + host + "-" + pid + ".values");
		System.setProperty("proposalfilename", "L" + host + "-" + pid + ".proposal");		
	}

	private final static Logger logger = Logger.getLogger(TTYNode.class);

	private final static Logger valuelogger = Logger.getLogger(Value.class);

	/**
	 * Thread that reads values from the standard input and proposes them to all
	 * the rings. 
	 */
	private static class StdinProposer implements Runnable {
		PaxosNode paxos;

		public StdinProposer(PaxosNode paxos) {
			this.paxos = paxos;
		}

		@Override
		public void run() {
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			String s;
			try {
				while ((s = in.readLine()) != null && s.length() != 0) {
					if (s.startsWith("exp")) {
						Thread t = new Thread(new Experiment1(paxos));
						t.setName("Experiment");
						t.run();
						continue;
					}
					// propose value read from stdin to ALL the rings
					for (RingDescription ring : paxos.getRings()) {
						if (s.contains("start")) {
							try {
								ProposerRole p = (ProposerRole) paxos.getProposer(ring.getRingID());
								p.setTestMode();
								for(int i=0;i<p.getConcurrentValues();i++){
									p.propose(p.getTestValue());
								}
							} catch (ClassCastException e){
								logger.error("Proposer did not support TestMode");
							}
						} else if (s.startsWith("!")) {
							if(paxos.getProposer(ring.getRingID()) != null){
								paxos.getProposer(ring.getRingID()).control(parseControl(s.replace("!","")));
							}else{
								logger.info("Node isn't a proposer for ring " + ring.getRingID());
							}
						} else {
							if(paxos.getProposer(ring.getRingID()) != null){
								paxos.getProposer(ring.getRingID()).propose(s.getBytes());
							}else{
								logger.info("Node isn't a proposer for ring " + ring.getRingID());
							}
						}
					}
				}
				in.close();
				System.exit(0); // used to stop properly in eclipse
			} catch (IOException e) {
				logger.error(e);
			}
		}
	}

	/**
	 * Thread that prints learned values to stdout.
	 */
	private static class StdoutLearner implements Runnable {
		PaxosNode paxos;

		public StdoutLearner(PaxosNode paxos) {
			this.paxos = paxos;
		}

		@Override
		public void run() {
			if (paxos.getLearner() == null) {
				return; // not a learner
			}
			while(true) {
				try {
					Decision d = paxos.getLearner().getDecisions().take();
					if(valuelogger.isDebugEnabled()){
						valuelogger.debug(d);
					}else if(valuelogger.isInfoEnabled() && !d.getValue().isSkip() && d.getValue().getValue().length > 0){
						valuelogger.info(d.getValue().asString());
					}
				} catch (InterruptedException e) {
					logger.error(e);
				}
			}
		}
	}

	protected static Control parseControl(String s){
		String[] token = s.split(",");
		if(token.length > 2){
			ControlType type = ControlType.Prepare;
			if(token[0].startsWith("s")){
				type = ControlType.Subscribe;
			}else if(token[0].startsWith("u")){
				type = ControlType.Unsubscribe;
			}
			return new Control(1,type,Integer.parseInt(token[1]),Integer.parseInt(token[2]));
		}else{
			return null;
		}
	}

	public static void main(String[] args) {
		String zoo_host = "127.0.0.1:2181";
		if (args.length > 2) {
			zoo_host = args[2];
		}
		int groupID = -1;
		if (args.length > 3) {
			groupID = Integer.parseInt(args[3]);
			logger.info("Node is in group " + groupID);
		}
		if (args.length < 2) {
			System.err.println("Plese use \"Node\" \"node ID\" \"ring ID:roles[;ring ID:roles]\" [zookeeper] [groupID] (eg. 1 1:PAL)");
		} else {
			int nodeID = Integer.parseInt(args[0]);
			List<RingDescription> rings = Util.parseRingsArgument(args[1]);
			// start paxos node
			final Node node = new Node(nodeID,groupID,zoo_host,rings);
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

			// start stdin learner
			Thread lt = new Thread(new StdoutLearner(node));
			lt.setName("StdoutLearner");
			lt.start();

			// start stdout proposer
			Thread pt = new Thread(new StdinProposer(node));
			pt.setName("StdinProposer");
			pt.start();
		}
	}
}
