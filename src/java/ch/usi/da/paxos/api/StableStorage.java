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

import ch.usi.da.paxos.storage.Decision;

/**
 * Name: StableStorage<br>
 * Description: <br>
 * 
 * Creation date: Feb 7, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public interface StableStorage {

	public void putBallot(Long instance, int ballot);
	
	public int getBallot(Long instance);
	
	public boolean containsBallot(Long instance);
	
	public void putDecision(Long instance, Decision decision);
	
	public Decision getDecision(Long instance);
	
	public boolean containsDecision(Long instance);
	
	public boolean trim(Long instance);
	
	public Long getLastTrimInstance();
	
	public void close();
}
