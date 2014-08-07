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

import ch.usi.da.paxos.message.Value;
	
/**
 * Name: Proposal<br>
 * Description: <br>
 * 
 * Creation date: Sep 5, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Proposal {

	private final long date = System.currentTimeMillis();

	private final Value value;
	
	/**
	 * @param v
	 */
	public Proposal(Value v){
		this.value = v;
	}
	
	/**
	 * @return the valuemessage
	 */
	public Value getValue(){
		return value;
	}
	
	/**
	 * @return the date
	 */
	public long getDate(){
		return date;
	}
}
