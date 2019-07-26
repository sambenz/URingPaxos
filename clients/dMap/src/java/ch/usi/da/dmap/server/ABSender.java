package ch.usi.da.dmap.server;
/* 
 * Copyright (c) 2017 Università della Svizzera italiana (USI)
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

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import ch.usi.da.dmap.thrift.gen.Command;
import ch.usi.da.dmap.thrift.gen.Dmap.Iface;
import ch.usi.da.dmap.thrift.gen.MapError;
import ch.usi.da.dmap.thrift.gen.Partition;
import ch.usi.da.dmap.thrift.gen.RangeCommand;
import ch.usi.da.dmap.thrift.gen.RangeResponse;
import ch.usi.da.dmap.thrift.gen.RangeType;
import ch.usi.da.dmap.thrift.gen.ReplicaCommand;
import ch.usi.da.dmap.thrift.gen.Response;
import ch.usi.da.dmap.thrift.gen.WrongPartition;
import ch.usi.da.dmap.utils.Utils;

/**
 * Name: ABSender<br>
 * Description: <br>
 * 
 * Creation date: Mar 03, 2017<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ABSender<K extends Comparable<K>,V> implements Iface {

	private final static Logger logger = Logger.getLogger(ABSender.class);
	
	private final DMapReplica<K,V> replica;
	
	public ABSender(DMapReplica<K,V> replica){
		this.replica = replica;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Response execute(Command cmd) throws MapError, WrongPartition, TException {
		logger.trace("ABSender received " + cmd);
		FutureResponse r = null;
		int send_ring = replica.partition_ring;
		switch(cmd.type){
		case CLEAR:
		case CONTAINSVALUE:
		case FIRSTKEY:
		case LASTKEY:
		case SIZE:
			r = new FutureResponse(replica.partitions.keySet());
			send_ring = replica.default_ring;
			break;
		case GET:
		case PUT:
		case PUTIFABSENT:
		case REPLACE:
		case REMOVE:
			r = new FutureResponse();
			break;
		}
		replica.getResponses().put(cmd.id,r);
		Response response = null;
		try {
			replica.getNode().getProposer(send_ring).propose(Utils.getBuffer(cmd).array());
			//logger.debug("ABSender wait on response ..." + cmd);
			List<Object> ol = r.getResponse();
			//logger.debug("... got response");
			for(Object o : ol){
				if(o instanceof MapError){
					throw (MapError)o;
				}else if(o instanceof WrongPartition){
					throw (WrongPartition)o;
				}else if(o instanceof TException){
					throw (TException)o;
				}
			}
			switch(cmd.type){
			case CONTAINSVALUE:
				response = ((Response)ol.get(0));
				for(Object o : ol){
					Response ret = (Response)o;
					if(ret.getCount() > 0){
						response = ret;
					}
				}
				break;
			case FIRSTKEY:
				response = ((Response)ol.get(0));
				try {
					for(Object o : ol){
						Response ret = (Response)o;
						if(response.getKey() != null && ret.getKey() != null){
							K o1 = (K)Utils.getObject(response.getKey());
							K o2 = (K)Utils.getObject(ret.getKey());
							if(o1 != null && o2 != null && o1.compareTo(o2) > 0){
								response = ret;
							}
						}
					}
				} catch (ClassNotFoundException e) {
					logger.error(e);
				}
				break;
			case LASTKEY:
				response = ((Response)ol.get(0));
				try {
					for(Object o : ol){
						Response ret = (Response)o;
						if(response.getKey() != null && ret.getKey() != null){
							K o1 = (K)Utils.getObject(response.getKey());
							K o2 = (K)Utils.getObject(ret.getKey());
							if(o1 != null && o2 != null && o1.compareTo(o2) < 0){
								response = ret;
							}
						}
					}
				} catch (ClassNotFoundException e) {
					logger.error(e);
				}
				break;
			case SIZE:
				long size = 0;
				for(Object o : ol){
					size += ((Response)o).getCount();
				}
				response = ((Response)ol.get(0)).setCount(size);
				break;
			case GET:
			case PUT:
			case PUTIFABSENT:
			case REPLACE:
			case REMOVE:
			case CLEAR:
				response = (Response)ol.get(0);
				break;
			}
		} catch (InterruptedException | IOException e) {
			throw new TException(e);
		}
		//logger.debug("ABSender return " + response);
		return response;
	}

	@Override
	public RangeResponse range(RangeCommand cmd) throws MapError, WrongPartition, TException {
		logger.trace("ABSender received " + cmd);
		if(cmd.getType() == RangeType.GETRANGE || cmd.getType() == RangeType.PARTITIONSIZE){ // direct response since based on consistent snapshot
			return replica.range(cmd.snapshot, cmd);
		}
		FutureResponse r = new FutureResponse();
		replica.getResponses().put(cmd.id,r);
		RangeResponse response = null;
		try {
			replica.getNode().getProposer(replica.default_ring).propose(Utils.getBuffer(cmd).array());
			Object o = r.getResponse().get(0);
			if(o instanceof MapError){
				throw (MapError)o;
			}else if(o instanceof WrongPartition){
				throw (WrongPartition)o;
			}else if(o instanceof TException){
				throw (TException)o;
			}
			response = (RangeResponse)o;
		} catch (InterruptedException | IOException e) {
			throw new TException(e);
		}
		return response;
	}

	@Override
	public Partition partition(long id) throws TException {
		Partition p = new Partition();
		p.setVersion(replica.partition_version);
		p.setPartitions(replica.partitions);
		return p;
	}

	@Override
	public void replica(ReplicaCommand cmd) throws TException {
		try {
			replica.getNode().getProposer(replica.default_ring).propose(Utils.getBuffer(cmd).array());
		} catch (IOException e) {
			throw new TException(e);
		}
	}

}
