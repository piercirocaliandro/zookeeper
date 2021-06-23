package it.uniroma2.txnsnaplog.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.txn.TxnHeader;

/* Return a list of Request, built for unit testing */

public class RequestFactoryUtil {
	
	public static List<Request> getRequestListForTesting(long size){
		List<Request> reqList = new ArrayList<>();
		int cxid = 0;
		int xid = 0;
		for(long i = 0; i < size; i++) {
			TxnHeader header = new TxnHeader(i, cxid, i, 1000L+i, 0);
			Stat stat = new Stat(i, i, 1000L+i, 1000L+i, 1, 1, 1, i, 10, 0, i);
			Request req = new Request(i, xid, 0, header, stat, i);
			
			reqList.add(req);
			cxid++;
			xid++;
		}
		
		return reqList;
	}
}
