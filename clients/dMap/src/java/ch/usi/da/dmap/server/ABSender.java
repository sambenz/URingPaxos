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

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import ch.usi.da.dmap.thrift.gen.Command;
import ch.usi.da.dmap.thrift.gen.Dmap.Iface;
import ch.usi.da.dmap.thrift.gen.MapError;
import ch.usi.da.dmap.thrift.gen.Partition;
import ch.usi.da.dmap.thrift.gen.RangeCommand;
import ch.usi.da.dmap.thrift.gen.RangeResponse;
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
public class ABSender implements Iface {

	private final static Logger logger = Logger.getLogger(ABSender.class);
	
	private final DMapReplica<?,?> replica;
	
	public ABSender(DMapReplica<?,?> replica){
		this.replica = replica;
	}
	
	@Override
	public Response execute(Command cmd) throws MapError, WrongPartition, TException {
		logger.debug("ABSender received " + cmd);
		FutureResponse r = new FutureResponse();
		replica.getResponses().put(cmd.id,r);
		Response response = null;
		try {
			replica.getNode().getProposer(replica.partition_ring).propose(Utils.getBuffer(cmd).array());
			Object o = r.getResponse();
			if(o instanceof MapError){
				throw (MapError)o;
			}else if(o instanceof WrongPartition){
				throw (WrongPartition)o;
			}else if(o instanceof TException){
				throw (TException)o;
			}
			response = (Response)o;
		} catch (InterruptedException | IOException e) {
			throw new TException(e);
		}
		return response;
	}

	@Override
	public RangeResponse range(RangeCommand cmd) throws MapError, WrongPartition, TException {
		logger.debug("ABSender received " + cmd);
		FutureResponse r = new FutureResponse();
		replica.getResponses().put(cmd.id,r);
		RangeResponse response = null;
		try {
			replica.getNode().getProposer(replica.default_ring).propose(Utils.getBuffer(cmd).array());
			Object o = r.getResponse();
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
		logger.debug("ABSender received partition cmd");
		Partition p = new Partition();
		p.setVersion(replica.partition_version);
		p.setPartitions(replica.partitions);
		/*FutureResponse r = new FutureResponse();
		replica.getResponses().put(id,r);
		Partition p = null;
		try {
			replica.getNode().getProposer(replica.default_ring).propose(Utils.getBuffer(id).array());
			p = (Partition) r.getResponse();
		} catch (InterruptedException | IOException e) {
			throw new TException(e);
		}*/
		return p;
	}

}
