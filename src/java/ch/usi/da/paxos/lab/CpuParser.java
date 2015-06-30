package ch.usi.da.paxos.lab;
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

public class CpuParser {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		File file = new File(args[0]);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		List<Float> cpu = new ArrayList<Float>();
		List<Float> gc = new ArrayList<Float>();
		String l;
		float f = 0;
		float g = 0;		
		while((l = reader.readLine()) != null){
			String[] xs = l.split(" ");
			if(xs[1].contains("Info")){
				if(f > 0){
					System.out.println(f);
					cpu.add(f);
				}
				if(g > 0){
					gc.add(g);
				}
				f = 0;
				g = 0;
			}else{
				if(xs[3].contains("TCP")){ //MultiRingLearner")){
					f = f + Float.parseFloat(xs[1]);
				}else if(xs[3].contains("GC")){
					g = g + Float.parseFloat(xs[1]);					
				}
			}
			
		}
		System.err.println(file.getAbsolutePath() + " avg:" + avg(cpu) + " max:" + max(cpu) + " GC:" + avg(gc));
		reader.close();
	}

	public static Float avg(List<Float> l){
		float a = 0;
		for(Float f : l){
			a = a + f;
		}
		return (float)a/l.size();
	}
	
	public static Float max(List<Float> l){
		float m = 0;
		for(Float f : l){
			if(f > m){
				m = f;
			}
		}
		return m;
	}
}
