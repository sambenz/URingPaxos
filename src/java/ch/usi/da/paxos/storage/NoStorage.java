package ch.usi.da.paxos.storage;

import ch.usi.da.paxos.api.StableStorage;
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
 * Name: NoStorage<br>
 * Description: <br>
 * 
 * Creation date: Feb 7, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class NoStorage implements StableStorage {

	//private final static Logger logger = Logger.getLogger(NoStorage.class);

	private int last_trimmed_instance = 0;
	
	@Override
	public void put(Integer instance, Decision decision) {
		/*if(logger.isDebugEnabled()){
			logger.debug("add " + decision + " to stable storage");
		}*/
	}

	@Override
	public Decision get(Integer instance) {
		return null;
	}

	@Override
	public boolean contains(Integer instance) {
		/*if(logger.isDebugEnabled()){
			logger.debug("check if " + instance + " exists in stable storage");
		}*/
		return false;
	}

	@Override
	public boolean trim(Integer instance) {
		last_trimmed_instance = instance;
		return true;
	}

	@Override
	public Integer getLastTrimInstance() {
		return last_trimmed_instance;
	}

	@Override
	public void close(){
		
	}
}
