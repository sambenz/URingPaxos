package ch.usi.da.paxos.lab;

import org.apache.commons.math3.random.RandomDataGenerator;

public class RandomDistribution {

	public static void main(String[] args) {
		RandomDataGenerator random = new RandomDataGenerator();
		System.out.println("Exp\tGauss\tZipf");
		for(int i=0;i<100;i++){
			System.out.println(Math.round(random.nextExponential(16000)) + "\t" 
			                  +Math.round(random.nextGaussian(16000, 14000)) + "\t"
			                  +random.nextZipf(60000,0.5));
		}
	}

}
