package it.uniroma2.datatree.utils;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.EphemeralType;

/* This class is thought to be extended by all the test classes that need to create some
 * nodes in the DataTree before a test */

public class DataTreeTestCommon {
	protected List<DataTreeNodeBean> getNodes(){
    	DataTreeNodeBean[] dtnbArray= {
    			new DataTreeNodeBean("/pierapp", "aaaa".getBytes(), 
    					"world:10:w", 1L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp1", "bbbb".getBytes(), 
    					"world:10:w", 1L, -1, 1L, 10L),
    			new DataTreeNodeBean("/pierapp/app1", "AAAA".getBytes(), 
    					"world:10:w", 1L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp/app2", "EPH2".getBytes(), 
    					"world:10:w", 1L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp1/app3", "BBBB".getBytes(), 
    					"world:10:w", 11L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp1/app4", "EPH4".getBytes(), 
    					"world:10:w", 11L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp1/app5", "EPH5".getBytes(), 
    					"world:10:w", 1L, 10, 1L, 10L),
    			
    			// these nodes were added to increase branch coverage
    			new DataTreeNodeBean("/contnode", "aaaa".getBytes(), 
    					"world:10:w", Long.MIN_VALUE, 10, 1L, 10L),
    			new DataTreeNodeBean("/zookeeper/quota/node1", "aaaa".getBytes(), 
    					"world:10:w", 0L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp3", "aaaa".getBytes(), 
    					"world:10:w", 10L, 10, 1L, 10L),
    			
    			new DataTreeNodeBean("/pierapp/pierapp_ttl", "aaaa".getBytes(), 
    					"world:10:w", new BigInteger("ff00000000000010", 16).longValue(), 
    					10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp_ttl2", "aaaa".getBytes(), 
    					"world:10:w", new BigInteger("ff00000000000100", 16).longValue(), 
    					10, 1L, 10L),
    			};
    	return Arrays.asList(dtnbArray);
    }
	
	
	protected void createNodes(List<DataTreeNodeBean> dnBeanList, DataTree dataTree) {
		for(DataTreeNodeBean nb : dnBeanList) {
			try {
				dataTree.createNode(nb.getPath(), nb.getData(), nb.getAcls(), nb.getEphemeralOwner(),
						nb.getParentCVers(), nb.getZxid(), nb.getTime());
			} catch (NoNodeException | NodeExistsException e) {
				Logger.getLogger("DTC").log(Level.SEVERE, "Failed to add node to the tree \n");
			}
		}
	}
}
