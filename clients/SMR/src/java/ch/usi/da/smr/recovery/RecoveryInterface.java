package ch.usi.da.smr.recovery;
/* 
 * Copyright (c) 2014 Università della Svizzera italiana (USI)
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

import java.util.Map;

/**
 * Name: RecoveryInterface<br>
 * Description: <br>
 * 
 * Creation date: Nov 6, 2014<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public interface RecoveryInterface {

	Map<Integer,Long> installState(String token,Map<String,byte[]> db) throws Exception;

	boolean storeState(Map<Integer, Long> exec_instance,Map<String,byte[]> db);
	
	void close();

}
