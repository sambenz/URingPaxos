package ch.usi.da.paxos.api;
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

/**
 * Name: PaxosRole<br>
 * Description: <br>
 * 
 * Creation date: Mar 31, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public enum PaxosRole {
	/**
	 * The proposer
	 */
	Proposer(0),
	/**
	 * The acceptors
	 */
	Acceptor(1),
	/**
	 * The learners
	 */
	Learner(2),
	/**
	 * The leader
	 */
	Leader(3);
	
	private final int id;
	private PaxosRole(int id) {
		this.id = id;
	}
	
	public int getId(){
		return id;
	}
	
	public static PaxosRole fromId(int id){
		for (PaxosRole r: values()){
			if (r.id == id) {
				return r;
			}
		}
		throw new RuntimeException("PaxosRole " + id + "does not exist!");
	}
}
