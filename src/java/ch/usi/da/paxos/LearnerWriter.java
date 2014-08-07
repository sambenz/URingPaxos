package ch.usi.da.paxos;
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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import ch.usi.da.paxos.storage.Decision;

/**
 * Name: LearnerWriter<br>
 * Description: <br>
 * 
 * Creation date: Apr 9, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class LearnerWriter implements Runnable {

	private final Learner learner;
	
	private final LinkedList<Decision> list = new LinkedList<Decision>();
	
	private AtomicLong next_instance = new AtomicLong(1);
	
	private AtomicLong max_seen_instance = new AtomicLong(0);
	
	private Set<Long> requested = new HashSet<Long>();
	
	private Set<String> delivered = new HashSet<String>(1000);
	
	/**
	 * Public constructor
	 * 
	 * @param learner
	 */
	public LearnerWriter(Learner learner){
		this.learner = learner;
	}
	
	@Override
	public void run() {
		int break_counter = 0;
		while(true){
			
			// fill list (sorted)
			try {
				Decision d = learner.getDecisions().poll(1,TimeUnit.SECONDS);
				if(d != null){
					if(requested.contains(d.getInstance())){
						requested.remove(d.getInstance());
					}
					if(d.getInstance().longValue() > max_seen_instance.get()){
						max_seen_instance.set(d.getInstance().longValue());
					}
					if(d.getInstance() == next_instance.get()){
						list.add(d);
						next_instance.incrementAndGet();
					}else{
						int pos = findPos(d.getInstance());
						if(pos >= 0){
							list.add(pos,d);
							if(pos == list.size()-1){
								next_instance.set(d.getInstance().longValue()+1);
							}
						}
					}
				}
			} catch (InterruptedException e) {
			}

			// request missing
			if((max_seen_instance.get() - learner.getInstance().get()) > 20){
				long first = max_seen_instance.get();
				int max = 10;
				if(!list.isEmpty()){
					first = list.get(0).getInstance().longValue();
				}
				int n = 0;
				for(long i = learner.getInstance().get();i<first;i++){
					if(!requested.contains(new Long(i))){
						requested.add(new Long(i));
						learner.getRequests().add(new Long(i));
						n++;
						if(n > max){ break; };
					}
				}
			}
			if(break_counter > 2){
				requested.remove(learner.getInstance().get());
				break_counter = 0;
			}
			
			// read list
			Iterator<Decision> i = list.iterator();
			while(i.hasNext()){
				Decision d = i.next();
				if(d.getInstance().longValue() == learner.getInstance().get()){
					i.remove();
					learner.getInstance().incrementAndGet();
					//System.out.println(d);
					if(d.getValue() != null && d.getValue().getValue().length>0){
						if(!delivered.contains(d.getValue().getID())){
							System.out.println(new String(d.getValue().getValue()));
							delivered.add(d.getValue().getID());
						}
					}
				}else if (d.getInstance().longValue() < learner.getInstance().get()){
					i.remove(); // duplicate
				}else{
					break_counter++;
					//System.err.println("queue:" + d.getInstance().longValue() + " server:" + learner.getInstance().get() + " break:" + break_counter);
					break; // since the list is sorted
				}
			}
		}
	}

	private int findPos(Long instance) {
		int pos = 0;
		Iterator<Decision> i = list.iterator();
		// ok, this would be possible in log(n) and not n ...
		while(i.hasNext()){
			Decision d = i.next();
			if(instance == d.getInstance().longValue()){
				return -1;
			}else if(instance < d.getInstance().longValue()){
				return pos;
			}
			pos++;
		}
		return pos;
	}

}
