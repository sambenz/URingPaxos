package ch.usi.da.dmap;
/* 
 * Copyright (c) 2017 Università della Svizzera italiana (USI)
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


import java.util.SortedMap;

/**
 * Name: DMap<br>
 * Description: <br>
 * 
 * Creation date: Feb 03, 2017<br>
 * $Id$
 *
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class DMap {

	public static void main(String[] args) {
		
		SortedMap<String, String> dmap = new DistributedOrderedMap<String, String>("83a8c1c0-dcb2-4afa-a447-07f79a0fcd6b","127.0.0.1:2181");
		
		//dmap.clear();
		System.out.println(dmap.put("Key1","9"));
		System.out.println(dmap.get("Key1"));
		dmap.put("Key2","9");
		dmap.put("Key3","10");
		System.out.println(dmap.containsValue("9"));
		System.out.println(dmap.firstKey());
		System.out.println(dmap.lastKey());

	}

}
