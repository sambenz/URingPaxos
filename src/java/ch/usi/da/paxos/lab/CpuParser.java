package ch.usi.da.paxos.lab;

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
		String l;
		float f = 0;
		while((l = reader.readLine()) != null){
			String[] xs = l.split(" ");
			if(xs[1].contains("Info")){
				if(f > 0){
					System.out.println(f);
					cpu.add(f);
				}
				f = 0;
			}else{
				f = f + Float.parseFloat(xs[1]);
			}
			
		}
		System.err.println(file.getAbsolutePath() + " " + avg(cpu));
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
