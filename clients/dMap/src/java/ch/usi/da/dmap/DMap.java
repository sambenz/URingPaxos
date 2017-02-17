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


import java.util.Map.Entry;
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
		
		/*SortedMap<String, String> local = new TreeMap<String,String>();
		local.put("a","1");
		local.put("b","2");
		local.put("c","3");
		local.put("d","4");
		local.put("e","5");
		local.put("f","6");
		local.put("g","7");
		local.put("z","8");
		System.out.println(local.subMap("a","b"));
		System.out.println(local.subMap("a","z"));
		System.out.println(local.subMap("b","e"));
		System.out.println(local.tailMap("b"));
		System.out.println(local.headMap("b"));
		
		System.out.println("###########");*/
		
		SortedMap<String, String> dmap = new DistributedOrderedMap<String, String>("83a8c1c0-dcb2-4afa-a447-07f79a0fcd6b","127.0.0.1:2181");
		dmap.put("a","1");
		dmap.put("b","2");
		dmap.put("c","3");
		dmap.put("d","4");
		dmap.put("e","5");
		dmap.put("f","6");
		dmap.put("g","7");
		dmap.put("z","8");
		System.out.println(dmap.subMap("a","b"));
		System.out.println(dmap.subMap("a","z"));
		System.out.println(dmap.subMap("b","e"));
		System.out.println(dmap.tailMap("b"));
		System.out.println(dmap.headMap("b"));
		
		
		for(Entry<String,String> e : dmap.entrySet()){
			System.out.println(e);
		}

	}

}
