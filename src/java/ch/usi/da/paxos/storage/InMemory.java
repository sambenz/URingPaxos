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

import java.util.LinkedHashMap;
import java.util.Map;

import ch.usi.da.paxos.api.StableStorage;


/**
 * Name: InMemory<br>
 * Description: <br>
 * 
 * Creation date: Mar 31, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class InMemory implements StableStorage {

	private final Map<Integer, Decision> decided = new LinkedHashMap<Integer,Decision>(10000,0.75F,false){
		private static final long serialVersionUID = -3704400228030327063L;
			protected boolean removeEldestEntry(Map.Entry<Integer, Decision> eldest) {  
				return size() > 15000; // hold only 15'000 values in memory !                                 
	}};
	
	@Override
	public void put(Integer instance, Decision decision) {
		decided.put(instance, decision);
	}

	@Override
	public Decision get(Integer instance) {
		return decided.get(instance);
	}

	@Override
	public boolean contains(Integer instance) {
		return decided.containsKey(instance);
	}

	@Override
	public boolean trim(Integer instance) {
		// not interesting since cyclic storage
		return true;
	}

	@Override
	public void close(){
		
	}
}
