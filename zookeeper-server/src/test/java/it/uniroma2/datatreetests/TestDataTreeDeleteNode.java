package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import it.uniroma2.datatree.utils.DataTreeTestCommon;

/* Perform a node removal from the DataTree, testing all the methods related */

@RunWith(value = Parameterized.class)
public class TestDataTreeDeleteNode extends DataTreeTestCommon{
	private String path;
	private long zxid;
	private DataTree dt;
	
	private Logger logger;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{"/pierapp", 1L},
			{"/pierapp", -1L},
			//{"/pierapp", Long.MAX_VALUE},
			//{"", 1L},
			{"/noparent/app23", 1L},
			//{null, 1L},
			{"/notpresent", 1L},
			
			//added to increase branch coverage
			{"/contnode", Long.MIN_VALUE},
			{"/zookeeper/quota/node1", 0L},
			{"/", -1L},
			{"/pierapp3", 1L},
			{"/pierapp_ttl2", -1L}
		});
	}
	
	
	public TestDataTreeDeleteNode(String path, long zxid) {
		this.configure(path, zxid);
	}
	
	
	private void configure(String path, long zxid) {
		
		// ZooKeeper system property to allow the creation of TTL nodes
		System.setProperty("zookeeper.extendedTypesEnabled", "true");
		
		this.path = path;
		this.zxid = zxid;
		
		this.logger = Logger.getLogger("RNT");
		
		this.dt = new DataTree();
		this.createNodes(this.getNodes(), this.dt);
	}
	
	
	@After
	public void cleanEnv() {
		System.setProperty("zookeeper.extendedTypesEnabled", "false");
	}
	
	
	@Test
	public void testRemoveNodes() {
		int nodesBeforeRem = this.dt.getNodeCount();
		try {
			this.dt.deleteNode(this.path, this.zxid);
			int nodesAfterRem = this.dt.getNodeCount();
			assertEquals(nodesBeforeRem-1, nodesAfterRem);
		} catch (NoNodeException e) {
			this.logger.log(Level.WARNING, "Failed to remove the node with given path:" + this.path);
		}
	}
}
