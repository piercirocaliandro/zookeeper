package it.uniroma2.datatree.utils;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.server.DataTree;

/* This class is thought to be extended by all the test classes that need to create some
 * nodes in the DataTree before a test */

public class DataTreeTestCommon {
	protected List<DataTreeNodeBean> getNodes(){
    	DataTreeNodeBean[] dtnbArray= {
    			new DataTreeNodeBean("/pierapp", "aaaa".getBytes(), 
    					"world:10:w", -1L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp1", "bbbb".getBytes(), 
    					"world:10:w", -1L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp/app1", "AAAA".getBytes(), 
    					"world:10:w", -1L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp/app2", "EPH2".getBytes(), 
    					"world:10:w", 2L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp1/app3", "BBBB".getBytes(), 
    					"world:10:w", -1L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp1/app4", "EPH4".getBytes(), 
    					"world:10:w", 3L, 10, 1L, 10L),
    			new DataTreeNodeBean("/pierapp1/app5", "EPH5".getBytes(), 
    					"world:10:w", 3L, 10, 1L, 10L)
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
