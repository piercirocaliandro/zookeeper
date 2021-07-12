package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.server.DataTree;
import org.junit.Before;
import org.junit.Test;

import it.uniroma2.datatree.utils.DataTreeNodeBean;
import it.uniroma2.datatree.utils.DataTreeTestCommon;

/* The the creation and retrieval of ephemeral nodes */

public class TestDataTreeEphemeralNodes extends DataTreeTestCommon{
	private DataTree dt;
	private List<DataTreeNodeBean> dtBeanList;
	private DataTreeNodeBean dtnBean;
	
	
	
	@Before
	public void configure() {
		this.dt = new DataTree();
		this.dtBeanList = this.getNodes();
		this.createNodes(this.dtBeanList, this.dt);
		this.dtnBean = new DataTreeNodeBean("/ephem", "aaaa".getBytes(), 
				"world:1:c", -1L, 10, 1L, 10L);
	}
	
	
	@Test
	public void testEphemNodes() {
		Map<Long, Set<String>> ephNodes = this.dt.getEphemerals();
    	Map<Long, Set<String>> ephNodes2 = new HashMap<>();
    	long currIndex = 0;
    	
    	assertFalse(ephNodes.isEmpty());
    	
    	for(DataTreeNodeBean db : this.dtBeanList) {
    		currIndex = db.getEphemeralOwner();
    		if(currIndex != -1)
    			ephNodes2.put(Long.valueOf(db.getEphemeralOwner()), 
    					this.dt.getEphemerals(db.getEphemeralOwner()));
    	}
    	for(Long id : ephNodes2.keySet()) {
    		if(ephNodes.containsKey(id)) {
    			for(String eph : ephNodes.get(id)) {
    				assertTrue(ephNodes2.get(id).contains(eph));
    			}
    		}
    	}
	}
	
	
	@Test
	public void testEphemCount() throws NoNodeException, NodeExistsException {
		int countBefore = this.dt.getEphemeralsCount();
		this.dt.createNode(this.dtnBean.getPath(), this.dtnBean.getData(), this.dtnBean.getAcls(), 
				this.dtnBean.getEphemeralOwner(), this.dtnBean.getParentCVers(), 
				this.dtnBean.getZxid(), this.dtnBean.getTime());
		
		int countAfter = this.dt.getEphemeralsCount();
		
		assertEquals(countBefore+1, countAfter, 0.0);
	}
	
	
	@Test
	public void testDumpEphems() throws IOException {
		File f = new File("epehm_dumps.txt");
		f.createNewFile();
		PrintWriter pw = new PrintWriter(f);
		
		long before = f.getFreeSpace();
		this.dt.dumpEphemerals(pw);
		
		assertEquals(before, f.getFreeSpace());
		
		f.delete();
	}
}
