package ch.usi.da.smr.statistics;
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

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.soda.StreamSelector;

/**
 * Name: Statistics<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Statistics implements UpdateListener {
	
	private final EPServiceProvider epService;
	
	public Statistics(){
        Configuration configuration = new Configuration();
        configuration.addEventType("Event", Event.class.getName());
        configuration.getEngineDefaults().getStreamSelection().setDefaultStreamSelector(StreamSelector.ISTREAM_ONLY);
        epService = EPServiceProviderManager.getProvider("PaxosStats", configuration);
        epService.initialize();

        String epl = "select count(*) as count, avg(latency) as latency, sum(size) as bytes from Event.win:time_batch(5 sec)";
        EPStatement stmt = epService.getEPAdministrator().createEPL(epl);
        stmt.addListener(this);

	}
	
	public void feedEvent(Event event){
		epService.getEPRuntime().sendEvent(event);
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		Long msg = (Long)newEvents[0].get("count");
		Double lat = (Double)newEvents[0].get("latency")/(1000*1000);
		Integer bytes = (Integer)newEvents[0].get("bytes");
		float deliver_bw = (float)8*(bytes/5)/1024/1024; // Mbit/s
		System.out.printf("%d m/s %.2f ms latency %.2f Mbit/s\n",msg,lat,deliver_bw);
	}
	
}
