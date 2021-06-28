package it.uniroma2.datatreetests;


import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import it.uniroma2.datatree.utils.DataTreeNodeBean;

@RunWith(value = Parameterized.class)
public class TestDataTreeNodeCreation {
	private DataTreeNodeBean dtNodeBean;
    private DataTree dt;
    private Stat stat; //used to test the second variant of the method createNode
    
	
    
    @Parameters
    public static Collection<Object[]> getParams(){
    	return Arrays.asList(new Object[][] {
    		{new DataTreeNodeBean("/pierapp", "aaaa".getBytes(), "world:10:w", 
    				-1L, 10, 1L, 1000L), new Stat()},
    		{new DataTreeNodeBean("/app", "".getBytes(), "ip:127.0.0.1:w", -1L, 1, 10L, 1000L),
    			new Stat()},
    		/*{new DataTreeNodeBean("", "abcd".getBytes(), "world:10:w", -1, 1, 10L, 1000L), 
    			new Stat()},*/
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "", -1L, 1, 10L, 1000L), new Stat()},
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "null", -1L, 1, 10L, 1000L), 
    			new Stat()},
    		//{new DataTreeNodeBean(null, "abcd".getBytes(), "world:1:c", -1, 1, 10L, 1000L), null},
    		//{new DataTreeNodeBean("/app/../", "abcd".getBytes(), "world:1:c", -1, 1, 10L, 1000L),
    			//new Stat()},
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", 3L, 1, 10L, 1000L), 
    			new Stat()},
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", Long.MAX_VALUE, 
    				Integer.MAX_VALUE, 10L, 1000L), new Stat()},
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", -1L, -1, 10L, 1000L), 
    					new Stat()},
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", 3L, Integer.MAX_VALUE, 
    				10L, 1000L), null},
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", 0L, 1, 
    				Long.MAX_VALUE, 1000L), null},
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", -1L, 1, -1L, 1000L),
    					null},
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", -1L, 1, 1L, -1L),
    			new Stat()},
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", -1L, 1, 1L, Long.MAX_VALUE),
    			new Stat()},
    		
    		// these were added to increase branch coverage
    		{new DataTreeNodeBean("/fakeparent/app", "abcd".getBytes(), "world:1:c", 
    				-1L, 1, 1L, 1L), new Stat()},
    		{new DataTreeNodeBean("/zookeeper", "abcd".getBytes(), "world:1:c", 
    				-1L, 1, 1L, 1L), new Stat()},
    		{new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", 
    				Long.MIN_VALUE, 1, 1L, 1L), new Stat()},
    		{new DataTreeNodeBean("/app", null, "world:1:c", 
    				2L, 1, 1L, Long.MAX_VALUE), new Stat()},
    		{new DataTreeNodeBean("/zookeeper/quota/app1", "abcd".getBytes(), "world:1:c", 
    				2L, 1, 1L, Long.MAX_VALUE), new Stat()},
    	});
    }
    
    
    public TestDataTreeNodeCreation(DataTreeNodeBean dtBean, Stat stat) {
    	this.configure(dtBean, stat);
    }
    
    
    // configure the system for the test case
    public void configure(DataTreeNodeBean dtBean, Stat stat) {
    	this.dtNodeBean = dtBean;
    	this.stat = stat;
    	
    	this.dt = new DataTree();
    }
    
    
    @Test
    public void testNodeCreation() {
    	int nodePrev = this.dt.getNodeCount();
    	try {
			this.dt.createNode(this.dtNodeBean.getPath(), this.dtNodeBean.getData(), 
					this.dtNodeBean.getAcls(), this.dtNodeBean.getEphemeralOwner(), 
					this.dtNodeBean.getParentCVers(), this.dtNodeBean.getZxid(), 
					this.dtNodeBean.getTime());
			assertEquals(nodePrev+1 , this.dt.getNodeCount());
	    	
	    	this.dt.deleteNode(this.dtNodeBean.getPath(), this.dtNodeBean.getZxid());
		} catch (NoNodeException | NodeExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
    @Test
    public void testNodeCreationWithStat() {
    	int nodePrev = this.dt.getNodeCount();
    	try {
			this.dt.createNode(this.dtNodeBean.getPath(), this.dtNodeBean.getData(), 
					this.dtNodeBean.getAcls(), this.dtNodeBean.getEphemeralOwner(), 
					this.dtNodeBean.getParentCVers(), this.dtNodeBean.getZxid(), 
					this.dtNodeBean.getTime(), this.stat);
			assertEquals(nodePrev+1 , this.dt.getNodeCount());
	    	
	    	this.dt.deleteNode(this.dtNodeBean.getPath(), this.dtNodeBean.getZxid());
		} catch (NoNodeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NodeExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
