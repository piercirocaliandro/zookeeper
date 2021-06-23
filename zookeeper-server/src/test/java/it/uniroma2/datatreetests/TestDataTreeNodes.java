package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DumbWatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import it.uniroma2.datatree.utils.DataTreeNodeBean;
import it.uniroma2.datatree.utils.DataTreeTestCommon;

/* This class tests the creation of a more structured DataTree. In particular, there are tests for:
 * 	- create the DataTree
 * 	- delete all the nodes in the DataTree
 * */

@RunWith(value = Parameterized.class)
public class TestDataTreeNodes extends DataTreeTestCommon{
	private DataTree dataTree;
	private List<DataTreeNodeBean> dnBeanList;
	
	private String path;
	private DumbWatcher dw;
	private Stat stat;
	private int newCVers;
	private long zxid;
    
    
    @Parameters
    public static Collection<Object[]> getParams(){
    	return Arrays.asList(new Object[][] {
    		{"/pierapp", new DumbWatcher(), new Stat(), 20, 1L},
    		{"/pierapp1", new DumbWatcher(), new Stat()},
    		{"/../..", new DumbWatcher(), new Stat()},
    		{null, new DumbWatcher(), new Stat()},
    		{"/pierapp", null, new Stat()},
    		{"/pierapp", new DumbWatcher(), null},
    	});
    }
    
    
    public TestDataTreeNodes(String path, DumbWatcher dw, Stat stat, int newCVers, long zxid) 
    		throws NoNodeException, NodeExistsException {
    	this.configure(path, dw, stat, newCVers, zxid);
    }
    
    
    public void configure(String path, DumbWatcher dw, Stat stat, int newCVers, long zxid) 
    		throws NoNodeException, NodeExistsException {
    	this.dnBeanList = this.getNodes();
    	this.dataTree = new DataTree();
    	
    	this.path = path;
    	this.dw = dw;
    	this.stat = stat;
    	this.newCVers = newCVers;
    	this.zxid = zxid;
    	
    	this.createNodes(this.dnBeanList, this.dataTree);
    }
    
    
    /* Utility function to get all the children for a given node */
    private List<String> getChildrenFromList(){
    	List<String> children = new ArrayList<>();
    	for(DataTreeNodeBean dtb : this.dnBeanList) {
    		String currNode = dtb.getPath();
    		if(currNode.startsWith(this.path) && currNode.length() > this.path.length()
    		&& currNode.charAt(this.path.length()) == '/')
    			children.add(currNode.substring(this.path.length()+1));
    	}
    	return children;
    }
    
    
    @Test
    public void testAllChildrenRetrieve() throws NoNodeException, NodeExistsException {
    	assertEquals(this.dataTree.getAllChildrenNumber(this.path), 
    			this.getChildrenFromList().size());
    }
    
    
    @Test
    public void testGetChildren() throws NoNodeException, NodeExistsException {
    	List<String> children = this.dataTree.getChildren(this.path, this.stat, this.dw);
    	List<String> retrChildren = this.getChildrenFromList();
    	
    	for(String child : retrChildren) {
    		assertTrue(children.contains(child));
    	}
    }
    
    
    @Test
    public void clearTree() throws NoNodeException {
    	int nodesPrev = this.dataTree.getNodeCount();
    	int removed = 0;
    	for(int i = this.dnBeanList.size()-1; i >= 0; i--) {
    		this.dataTree.deleteNode(this.dnBeanList.get(i).getPath(), 
    				this.dnBeanList.get(i).getZxid());
    		removed++;
    	}
    	assertEquals(nodesPrev-removed, this.dataTree.getNodeCount());
    }
}
