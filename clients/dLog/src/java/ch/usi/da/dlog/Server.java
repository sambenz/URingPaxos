package ch.usi.da.dlog;
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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import ch.usi.da.dlog.message.Command;
import ch.usi.da.dlog.message.CommandType;
import ch.usi.da.dlog.message.Message;
import ch.usi.da.dlog.transport.ABListener;
import ch.usi.da.dlog.transport.RawABListener;
import ch.usi.da.dlog.transport.Receiver;
import ch.usi.da.dlog.transport.UDPSender;
import ch.usi.da.paxos.examples.Util;
import ch.usi.da.paxos.ring.RingDescription;

/**
 * Name: Server<br>
 * Description: <br>
 * 
 * Creation date: Apr 07, 2014<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Server implements Receiver {
	static {
		// get hostname and pid for log file name
		String host = "localhost";
		try {
			Process proc = Runtime.getRuntime().exec("hostname");
			BufferedInputStream in = new BufferedInputStream(proc.getInputStream());
			byte [] b = new byte[in.available()];
			in.read(b);
			in.close();
			host = new String(b).replace("\n","");
		} catch (IOException e) {
		}
		int pid = 0;
		try {
			pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
		} catch (NumberFormatException | IOException e) {
		}
		System.setProperty("logfilename", host + "-" + pid + ".log");
	}

	private final static Logger logger = Logger.getLogger(Server.class);

	private final List<RingDescription> rings;
	
	private final UDPSender udp;

	private final ABListener ab;

	private long next_position = 0;

	private final int read_size = 5;
	
	private SeekableByteChannel log;
	
	private final ByteBuffer buffer = ByteBuffer.allocate(200*1024*1024); // 200 MB

	private final int compact_size = (int)buffer.capacity()/5; // 20%

	private long buffer_offset = next_position;
	
	@SuppressWarnings("unchecked")
	private final TreeMap<Long,Long>[] index = new TreeMap[20]; // max ringID = 20

	private long last_trim = -1;
	
	public Server(List<RingDescription> rings, String zoo_host) throws Exception {
		this.rings = rings;
		udp = new UDPSender();
		ab = new RawABListener(zoo_host,rings);
        String path = "/tmp";
		String db_path = System.getenv("DLOG");
		if(db_path != null){
			path = db_path;
		}
		Path file = Paths.get(path + "/dlog-0.bin");
		log = Files.newByteChannel(file, EnumSet.of(CREATE, TRUNCATE_EXISTING, WRITE, READ));
		for(RingDescription ring : rings){
			index[ring.getRingID()] = new TreeMap<Long,Long>();
		}
	}

	public void start(){
		ab.registerReceiver(this);
		Thread t = new Thread((Runnable) ab);
		t.setName("ABListener");
		t.start();
	}

	public void close(){
		ab.close();
	}

	@Override
	public synchronized void receive(Message m) {
		logger.debug("Server received ring " + m.getRing() + " instnace " + m.getInstnce() + " (" + m + ")");
		if(m.isSkip()){
			return;
		}
		List<Command> cmds = new ArrayList<Command>();		
		for(Command c : m.getCommands()){
			Command cmd;
	    	switch(c.getType()){
	    	case APPEND:
	    	case MULTIAPPEND:
	    		// index
	    		index[m.getRing()].put(next_position,m.getInstnce());
	    		// file
	    		try {
	    			log.position(next_position - last_trim);
					log.write(ByteBuffer.wrap(c.getValue()));
				} catch (IOException e) {
					logger.error("Error writing to seekable byte channel!",e);
				}
	    		// buffer
	    		buffer.position((int)(next_position - buffer_offset));
	    		if(buffer.remaining() < c.getValue().length){ // compact if no space
	    			logger.debug("Buffer compaction required!");	
	    			buffer_offset += compact_size;
	    			buffer.position(compact_size);
	    			buffer.compact();
	    		}
	    		buffer.position((int)(next_position - buffer_offset));
	    		buffer.put(c.getValue());
	    		
		    	cmd = new Command(c.getID(),CommandType.RESPONSE,next_position,new byte[0]);
		    	cmds.add(cmd);
		    	next_position += c.getValue().length;		    			
	    		break;
			case READ:
				if(c.getPosition() < last_trim){
					return;
				}
				byte[] b = new byte[0];
				int to_read = read_size;
				if(c.getValue().length > 0){
					to_read = Integer.parseInt(new String(c.getValue()));
				}else{
					to_read = read_size;
				}
				if((c.getPosition() + to_read) >= next_position){
					to_read = (int) (next_position - c.getPosition());
				}
				if(to_read > 0){
					b = new byte[to_read];
				}else{
					logger.error("read_size is <= 0!");
					return;
				}
				// file
				if(c.getPosition() < buffer_offset){
					logger.warn("Read position from disk!");
					try {
						log.position(c.getPosition()-last_trim);
						log.read(ByteBuffer.wrap(b));
					} catch (IOException e) {
						logger.error("error reading from seekable byte channel!",e);
					}
				}else{ // buffer
					int buffer_position = (int)(c.getPosition()-buffer_offset);
					if((buffer_position + to_read) <= buffer.capacity()){
						buffer.position((int)(c.getPosition()-buffer_offset));
						buffer.get(b);
					}else{
						logger.error("requested position is bigger than buffer!");
					}
				}
				cmd = new Command(c.getID(),CommandType.RESPONSE,c.getPosition(),b);
				cmds.add(cmd);
				break;
			case TRIM:
				try {
					Path file = Paths.get("/tmp/dlog-" + c.getPosition() + ".bin");
					SeekableByteChannel log2 = Files.newByteChannel(file, EnumSet.of(CREATE, TRUNCATE_EXISTING, WRITE, READ));
					((FileChannel)log).transferTo(c.getPosition()+1,log.size(),log2);
					log.close(); // maybe also delete?
					log = log2;
					for(RingDescription ring : rings){
						System.err.println(index[ring.getRingID()]);
						long inc_pos = index[ring.getRingID()].floorKey(c.getPosition());
						Entry<Long, Long> pos = index[ring.getRingID()].floorEntry(inc_pos-1);
						long instance = pos.getValue();
						if(instance > 0){
							ab.safe(ring.getRingID(),instance);
							logger.debug("set safe ring " + ring.getRingID() + " to instance " + instance);
						}
						index[ring.getRingID()] = new TreeMap<Long,Long>(index[ring.getRingID()].tailMap(pos.getKey()));
					}
					last_trim = c.getPosition();
					cmd = new Command(c.getID(),CommandType.RESPONSE,c.getPosition(),new byte[0]);
					cmds.add(cmd);
				} catch (Exception e) {
					logger.error("Error trimming file channel!",e);
				}
				break;
			default:
				System.err.println("Receive RESPONSE as Command!"); break;
	    	}
		}
		logger.debug("offset: " + next_position + " buffer_pos: " + buffer.position() + " buffer_offset: " + buffer_offset);
		int msg_id = new String(m.getInstnce() + "-" +  next_position).hashCode();
		Message msg = new Message(msg_id,rings.get(0).toString(),m.getFrom(),cmds);
		udp.send(msg);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String zoo_host = "127.0.0.1:2181";
		if (args.length > 1) {
			zoo_host = args[1];
		}
		if (args.length < 1) {
			System.err.println("Plese use \"Server\" \"ring ID,node ID:roles[;ring,ID:roles]\" (eg. 1,1:PAL) [zookeeper host]");
		} else {
			try {
				final Server server = new Server(Util.parseRingsArgument(args[0]),zoo_host);
				Runtime.getRuntime().addShutdownHook(new Thread("ShutdownHook"){
					@Override
					public void run(){
						server.close();
					}
				});
				server.start();
				BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
				in.readLine();
				server.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
}
