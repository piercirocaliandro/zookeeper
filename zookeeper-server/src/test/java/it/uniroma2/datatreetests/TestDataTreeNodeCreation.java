package it.uniroma2.datatreetests;


import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
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
    
	
    
    @Parameters
    public static Collection<DataTreeNodeBean> getParams(){
    	return Arrays.asList(new DataTreeNodeBean[] {
    		new DataTreeNodeBean("/pierapp", "aaaa".getBytes(), "world:10:w", 
    				-1L, 10, 1L, 1000L),
    		new DataTreeNodeBean("/app", "".getBytes(), "ip:127.0.0.1:w", -1, 1, 10L, 1000L),
    		new DataTreeNodeBean("", "abcd".getBytes(), "world:10:w", -1, 1, 10L, 1000L),
    		new DataTreeNodeBean("/app", "abcd".getBytes(), "", -1, 1, 10L, 1000L),
    		new DataTreeNodeBean("/app", "abcd".getBytes(), "null", -1, 1, 10L, 1000L),
    		new DataTreeNodeBean(null, "abcd".getBytes(), "world:1:c", -1, 1, 10L, 1000L),
    		new DataTreeNodeBean("/app/../", "abcd".getBytes(), "world:1:c", -1, 1, 10L, 1000L),
    		new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", 3, 1, 10L, 1000L),
    		new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", Integer.MAX_VALUE, 
    				1, 10L, 1000L),
    		new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", -1, -1, 10L, 1000L),
    		new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", 3, Integer.MAX_VALUE, 
    				10L, 1000L),
    		new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", -1, 1, 
    				Long.MAX_VALUE, 1000L),
    		new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", -1, 1, -1L, 1000L),
    		new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", -1, 1, 1L, -1L),
    		new DataTreeNodeBean("/app", "abcd".getBytes(), "world:1:c", -1, 1, 1L, Long.MAX_VALUE),
    	});
    }
    
    
    public TestDataTreeNodeCreation(DataTreeNodeBean dtBean) {
    	this.configure(dtBean);
    }
    
    
    // configure the system for the test case
    public void configure(DataTreeNodeBean dtBean) {
    	this.dtNodeBean = dtBean;
    	this.dt = new DataTree();
    }
    
    
    @Test
    public void testNodeCreation() throws NoNodeException, NodeExistsException{
    	int nodePrev = this.dt.getNodeCount();
    	this.dt.createNode(this.dtNodeBean.getPath(), this.dtNodeBean.getData(), 
    			this.dtNodeBean.getAcls(), this.dtNodeBean.getEphemeralOwner(), 
    			this.dtNodeBean.getParentCVers(), this.dtNodeBean.getZxid(), 
    			this.dtNodeBean.getTime());
    	
    	assertEquals(nodePrev+1 , this.dt.getNodeCount());
    }
}
