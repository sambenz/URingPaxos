package ch.usi.da.dlog;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LogParser {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		File file = new File(args[0]);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String l;
		List<Float> msgl = new ArrayList<Float>();
		String latency = null;
		int skip = 5;
		while((l = reader.readLine()) != null){
			String[] xs = l.split(" ");
			if(l.contains("Client:") && l.contains("sent")){ // learner throughput
				float msg = Float.parseFloat(xs[7]);
				if(skip > 0){
					skip--;
				}else{
					msgl.add(msg);
				}
			}else if (l.contains("ClientStatsWriter")){
				float msg = Float.parseFloat(xs[9]);
				if(skip > 0){
					skip--;
				}else{
					msgl.add(msg);
				}
			}else if (l.contains("histogram")){
				latency = l.substring(58);
				break;
			}
		}
		if(msgl.size()>0){
			msgl.remove(msgl.size()-1);
			System.out.println(file.getAbsolutePath() + " " + avg(msgl) + " ops/s");			
		}
		if(latency != null){
			System.out.println(file.getAbsolutePath() + " " + latency);
		}
		reader.close();
	}

	public static Float avg(List<Float> l){
		float a = 0;
		for(Float f : l){
			a = a + f;
		}
		return (float)a/l.size();
	}
}
