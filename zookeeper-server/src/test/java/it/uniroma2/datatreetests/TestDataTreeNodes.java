package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	private Logger logger;
    
    
    @Parameters
    public static Collection<Object[]> getParams(){
    	return Arrays.asList(new Object[][] {
    		{"/pierapp", new DumbWatcher(), new Stat()},
    		{"/pierapp1", new DumbWatcher(), new Stat()},
    		{"/pierapp", null, new Stat()},
    		{"/pierapp", new DumbWatcher(), null},
    		
    		{"/", new DumbWatcher(), null},
    		{"/nonode", new DumbWatcher(), new Stat()},
    	});
    }
    
    
    public TestDataTreeNodes(String path, DumbWatcher dw, Stat stat) 
    		throws NoNodeException, NodeExistsException {
    	this.configure(path, dw, stat);
    }
    
    
    public void configure(String path, DumbWatcher dw, Stat stat) 
    		throws NoNodeException, NodeExistsException {
    	this.dnBeanList = this.getNodes();
    	this.dataTree = new DataTree();
    	
    	this.path = path;
    	this.dw = dw;
    	this.stat = stat;
    	
    	this.logger = Logger.getLogger("LOG");
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
    
    
    private int getNumChildrenFromList(){
    	int num = 0;
    	if(this.path.equals("/"))
    		return this.dnBeanList.size();
    	
    	for(DataTreeNodeBean dtb : this.dnBeanList) {
    		String currNode = dtb.getPath();
    		if(currNode.startsWith(this.path) && currNode.length() > this.path.length()
    		&& currNode.charAt(this.path.length()) == '/')
    			num++;
    	}
    	return num;
    }
    
    
    @Test
    public void testAllChildrenRetrieve() {
    	int size = 0;
    	if(this.path.equals("/"))
    		size = 3;
    	size += this.getNumChildrenFromList();
    	assertEquals(this.dataTree.getAllChildrenNumber(this.path), size);
    }
    
    
    @Test
    public void testGetChildren() {
    	List<String> children;
		try {
			children = this.dataTree.getChildren(this.path, this.stat, this.dw);
			List<String> retrChildren = this.getChildrenFromList();
	    	
	    	for(String child : retrChildren) {
	    		assertTrue(children.contains(child));
	    	}
		} catch (NoNodeException e) {
			this.logger.log(Level.SEVERE, "Error while fetching children \n");
		}
    }
}
