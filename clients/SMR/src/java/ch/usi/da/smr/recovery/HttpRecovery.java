package ch.usi.da.smr.recovery;
/* 
 * Copyright (c) 2014 Università della Svizzera italiana (USI)
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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import ch.usi.da.smr.PartitionManager;
import ch.usi.da.smr.Replica;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Name: HttpRecovery<br>
 * Description: <br>
 * 
 * Creation date: Nov 6, 2014<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class HttpRecovery implements RecoveryInterface {

	private final static Logger logger = Logger.getLogger(HttpRecovery.class);

	private final PartitionManager partitions;
	
	private final int max_ring = 20;

	private HttpServer httpd;

	public static final String snapshot_file = "/tmp/snapshot.ser";

	public static final String state_file = "/tmp/snapshot.state"; // only used to quickly decide if remote snapshot is newer

	private boolean download_active = false;
	
	public HttpRecovery(PartitionManager partitions){
		
		this.partitions = partitions;
		
		// remote snapshot transfer server
		try {
			httpd = HttpServer.create(new InetSocketAddress(8080), 0);
			httpd.createContext("/state", new SendFile(state_file));        
			httpd.createContext("/snapshot", new SendFile(snapshot_file));
			httpd.setExecutor(null); // creates a default executor
			httpd.start();
		}catch(Exception e){
			logger.error("Replica could not start http server: " + e);
		}
	}
	
	public boolean storeState(Map<Integer,Long> instances, Map<String,byte[]> db){
		try {
			synchronized(db){
				logger.info("Replica start storing state ... ");
				for(Entry<Integer,Long> e : instances.entrySet()){
					db.put("r:" + e.getKey(),e.getValue().toString().getBytes());
				}
				FileOutputStream fs = new FileOutputStream(snapshot_file);
		        ObjectOutputStream os = new ObjectOutputStream(fs);
		        os.writeObject(db);
		        os.flush();
		        fs.getChannel().force(false); // fsync
		        os.close();
				fs = new FileOutputStream(state_file);
				for(Entry<Integer, Long> e : instances.entrySet()){
					fs.write((e.getKey() + "=" + e.getValue() + "\n").getBytes());
				}
				fs.getChannel().force(false);
		        os.close();
		        logger.info("... state stored up to instance: " + instances);
			}
	        return true;
		} catch (IOException e) {
			logger.error(e);
		}
		return false;
	}
	
	public Map<Integer,Long> installState(String token,Map<String,byte[]> db) throws Exception {
		Map<Integer,Long> instances = new HashMap<Integer,Long>();
		if(download_active){
			logger.info("Install state surpressed since download active.");
			return instances;
		}
		String host = null;
		InputStreamReader isr;
		String line;
		Map<Integer, Long> state = new HashMap<Integer, Long>();	
		// local
		try{
			isr = new InputStreamReader(new FileInputStream(state_file));
			BufferedReader bin = new BufferedReader(isr);	
			while ((line = bin.readLine()) != null){
				String[] s = line.split("=");
				state.put(Integer.parseInt(s[0]),Long.parseLong(s[1]));
			}
			logger.info("Replica found local snapshot: " + state);
			bin.close();
		}catch (FileNotFoundException e){
			logger.info("No local snapshot present.");
		}
		// remote TODO: must ask min. GSQ replicas
		for(String h : partitions.getReplicas(token)){
			if(h.contains(":")){
				h = "[" + h + "]";
			}
			try{
				URL url = new URL("http://" + h + ":8080/state");
				HttpURLConnection con = (HttpURLConnection)url.openConnection();
				isr = new InputStreamReader(con.getInputStream());
				BufferedReader in = new BufferedReader(isr);
				Map<Integer, Long> nstate = new HashMap<Integer, Long>();
				while ((line = in.readLine()) != null){
					String[] s = line.split("=");
					nstate.put(Integer.parseInt(s[0]),Long.parseLong(s[1]));
				}
				if(state.isEmpty()){
					state = nstate;
					host = h;
				}else if(Replica.newerState(nstate,state)){
					state = nstate;
					host = h;
				}
				logger.info("Replica found remote snapshot: " + nstate + " (" + h + ")");
				in.close();
			}catch(Exception e){
				logger.error("Error getting state from " + h,e);
			}
		}
		InputStream in;
		if(host != null){
			logger.info("Use remote snapshot from host " + host);
			download_active = true;
			URL url = new URL("http://" + host + ":8080/snapshot");
			HttpURLConnection con = (HttpURLConnection)url.openConnection();
			logger.info("Open remote host " + host + " size:" + con.getContentLengthLong() + " timeout:" + con.getReadTimeout());
			in = con.getInputStream();
		}else{
			in = new FileInputStream(new File(snapshot_file));
		}
		ObjectInputStream ois = new ObjectInputStream(in);
		logger.info("Start reading remote snapshot ... ");
		@SuppressWarnings("unchecked")
		Map<String,byte[]> m = (Map<String,byte[]>) ois.readObject();
		logger.info(" ... read " + m.size() + " entries.");
		ois.close();
		in.close();
		synchronized(db){
			db.clear();
			db.putAll(m);
			byte[] b = null;
			for(int i = 1;i<max_ring+1;i++){
				if((b = db.get("r:" + i)) != null){
					instances.put(i,Long.valueOf(new String(b)));
				}
			}
		}
		logger.info("Replica installed snapshot instance: " + instances);
		download_active = false;
		return instances;
	}

	public void close(){
		if(httpd != null){
			httpd.stop(1);
		}
	}

    public static class SendFile implements HttpHandler {
    	
    	private final File file;
    	
    	public SendFile(String file){
    		this.file = new File(file);
    	}
    	
        public void handle(HttpExchange t) throws IOException {
            FileInputStream fis = new FileInputStream(file);
            BufferedInputStream bis = new BufferedInputStream(fis);

        	byte[] b = new byte[(int)file.length()];
            bis.read(b, 0, b.length); //TODO: FIXME: keeps file in mem!
            bis.close();
            fis.close();

            t.sendResponseHeaders(200, b.length);
            OutputStream os = t.getResponseBody();
            os.write(b);
            os.close();
        }
    }

}
