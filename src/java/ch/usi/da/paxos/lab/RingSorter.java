package ch.usi.da.paxos.lab;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.ring.RingDescription;

/**
 * Name: RingSorter<br>
 * Description: <br>
 * 
 * Sorts a ring latency optimal:
 * 1) all proposer only
 * 2) proposers with acceptors
 * 3) acceptors
 * 4) acceptors with learners
 * 5) learners
 * 
 * Creation date: Aug 14, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class RingSorter implements Comparator<Integer> {

	private final List<Integer> p = new ArrayList<Integer>();
	private final List<Integer> a = new ArrayList<Integer>();
	private final List<Integer> l = new ArrayList<Integer>();
	
	
	/**
	 * @param ring
	 * 
	 */
	public RingSorter(List<RingDescription> ring){
		for(RingDescription r : ring){
			if(r.getRoles().contains(PaxosRole.Proposer)){
				p.add(r.getNodeID());
			}
			if(r.getRoles().contains(PaxosRole.Acceptor)){
				a.add(r.getNodeID());
			}
			if(r.getRoles().contains(PaxosRole.Learner)){
				l.add(r.getNodeID());
			}
		}

	}
	
	private boolean isP(int i){
		return p.contains(i);
	}

	private boolean isA(int i){
		return a.contains(i);
	}

	private boolean isL(int i){
		return l.contains(i);
	}

	@Override
	public int compare(Integer i1, Integer i2) {
		if(!isP(i1) && !isA(i1) && !isL(i1)){
			if(i1 < i2){
				return -1;
			}else if(i2 > i1){
				return 1;
			}
			return 0;
		}
		
		// both learners / acceptors
		if(isL(i1) && isL(i2) && isA(i1) && isA(i2)){
			if(i1 < i2){
				return -1;
			}else if(i2 > i1){
				return 1;
			}
			return 0;
		}
		if(isL(i1) && isL(i2) && isA(i1) && !isA(i2)){
			return -1;
		}
		if(isL(i1) && isL(i2) && !isA(i1) && isA(i2)){
			return 1;
		}
		if(isL(i1) && isL(i2)){
			if(i1 < i2){
				return -1;
			}else if(i2 > i1){
				return 1;
			}
			return 0;
		}

		// one learner
		if(isL(i1) && !isL(i2)){
			return 1;
		}
		if(!isL(i1) && isL(i2)){
			return -1;
		}

		// both proposer / acceptors
		if(isP(i1) && isP(i2) && isA(i1) && isA(i2)){
			if(i1 < i2){
				return -1;
			}else if(i2 > i1){
				return 1;
			}
			return 0;
		}
		if(isP(i1) && isP(i2) && isA(i1) && !isA(i2)){
			return 1;
		}
		if(isP(i1) && isP(i2) && !isA(i1) && isA(i2)){
			return -1;
		}
		if(isP(i1) && isP(i2)){
			if(i1 < i2){
				return -1;
			}else if(i2 > i1){
				return 1;
			}
			return 0;
		}
		
		// one proposer
		if(isP(i1) && !isP(i2)){
			return -1;
		}
		if(!isP(i1) && isP(i2)){
			return 1;
		}

		return 0;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		List<RingDescription> ring = new ArrayList<RingDescription>();
		
		List<PaxosRole> n1 = new ArrayList<PaxosRole>();
		n1.add(PaxosRole.Proposer);
		n1.add(PaxosRole.Acceptor);
		//n1.add(PaxosRole.Learner);
		ring.add(new RingDescription(1,1,n1));

		List<PaxosRole> n2 = new ArrayList<PaxosRole>();
		n2.add(PaxosRole.Proposer);
		//n2.add(PaxosRole.Acceptor);
		//n2.add(PaxosRole.Learner);
		ring.add(new RingDescription(1,2,n2));

		List<PaxosRole> n3 = new ArrayList<PaxosRole>();
		//n3.add(PaxosRole.Proposer);
		//n3.add(PaxosRole.Acceptor);
		n3.add(PaxosRole.Learner);
		ring.add(new RingDescription(1,3,n3));

		List<Integer> r = new ArrayList<Integer>();
		r.add(3);
		r.add(2);
		r.add(1);
		//r.add(5);
		
		// don't use it in the ring manager! it is quite tricky to have a
		// consistent view of four zookeeper directories! 
		Collections.sort(r,new RingSorter(ring));
		
		System.out.println(r);
	}

}
