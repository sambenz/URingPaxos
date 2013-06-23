package ch.usi.da.paxos.storage;
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

import java.io.Serializable;

import ch.usi.da.paxos.message.Value;

/**
 * Name: Decision<br>
 * Description: <br>
 * 
 * Creation date: Apr 2, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class Decision implements Serializable {
	
	private static final long serialVersionUID = -2916694736282875646L;

	private final Integer instance;
	
	private Integer ballot;
	
	private final Value value;
	
	/**
	 * @param instance
	 * @param ballot
	 * @param value
	 */
	public Decision(Integer instance,Integer ballot,Value value){
		this.ballot = ballot;
		this.instance = instance;
		this.value = value;
	}
	
	/**
	 * @return the ballot
	 */
	public synchronized Integer getBallot() {
		return ballot;
	}

	/**
	 * @param ballot the ballot to set
	 */
	public synchronized void setBallot(Integer ballot) {
		this.ballot = ballot;
	}

	/**
	 * @return the instance
	 */
	public Integer getInstance() {
		return instance;
	}

	/**
	 * @return the value
	 */
	public Value getValue() {
		return value;
	}
	
	public String toString(){
		return("decided to: " + this.getValue() + " (ballot " + this.getBallot() + " in instance " + this.getInstance() + ")");
	}
	
	public boolean equals(Object obj) {
		if(obj instanceof Decision){
			Decision d = (Decision) obj;
            if(this.getInstance().equals(d.getInstance()) && this.getValue().equals(d.getValue())){
                    return true;
            }
		}
		return false;
	}
	
	public int hashCode() {
		return this.getInstance().intValue();
	}	
}
