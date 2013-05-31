package ch.usi.da.paxos.lab;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Name: RingSorter<br>
 * Description: <br>
 * 
 * Creation date: Aug 30, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class RingSorter {

	List<Integer> ring = new ArrayList<Integer>();
	
	/**
	 * 
	 */
	public RingSorter(){
	}
	
	/**
	 * 
	 */
	public void sort(){

		// sort P id and add
		List<Integer> p = new ArrayList<Integer>();
		//p.add(5);
		//p.add(2);
		p.add(8);
		Collections.sort(p);
		for(Integer i : p){
			if(!ring.contains(i)){
				ring.add(i);
			}
		}
		
		// sort A id and add if not in
		List<Integer> a = new ArrayList<Integer>();
		a.add(5);
		//a.add(2);
		//a.add(8);
		Collections.sort(a);
		for(Integer i : a){
			if(!ring.contains(i)){
				ring.add(i);
			}
		}

		// sort L id and add if not in
		List<Integer> l = new ArrayList<Integer>();
		l.add(5);
		l.add(2);
		l.add(8);
		Collections.sort(l);
		for(Integer i : l){
			if(!ring.contains(i)){
				ring.add(i);
			}
		}
						
		System.out.println(ring);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		RingSorter sorter = new RingSorter();
		sorter.sort();
	}
}
