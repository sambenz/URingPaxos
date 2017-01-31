package ch.usi.da.dmap.local;

import java.util.Random;

public class TreeRunner<K extends Comparable<K>,V> implements Runnable {

	private final Btree<K,V> tree;
	
	public TreeRunner(Btree<K,V> tree){
		this.tree = tree;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		Random rnd = new Random();

		for(int i=1;i<10000;i++){
			//System.err.println(i);
			//System.err.println(tree);
			int k = rnd.nextInt(100000);
			//System.out.println(k);
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(k % 2 == 0){
				((Btree<Integer,String>)tree).put(k,Integer.toString(k));
			}else{
				((Btree<Integer,String>)tree).get(k);
			}
		}

	}

}
