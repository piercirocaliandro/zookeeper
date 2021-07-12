package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DumbWatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/* Test all the methods regarding Stat objects*/

@RunWith(value = Parameterized.class)
public class TestDataTreeStats {
	private long zxid;
	private long time;
	private long ephemOwner;
	
	private String path;
	private DumbWatcher watcher;
	private DataTree dt;
	private Logger logger;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{1L, 1000L, 1L, "/node", new DumbWatcher()},
			{-1L, 1000L, 1L, "/node", new DumbWatcher()},
			{1L, -1000L, 1L, "/node", new DumbWatcher()},
			{1L, 1000L, -1L, "/node", new DumbWatcher()},
			{Long.MAX_VALUE, 1000L, 1L, "/node", new DumbWatcher()},
			{1L, Long.MAX_VALUE, 1L, "/node", new DumbWatcher()},
			{1L, 1000L, Long.MAX_VALUE, "/node", new DumbWatcher()},
			{1L, 1000L, 1L, "", new DumbWatcher()},
			{1L, 1000L, 1L, "/node", null},
			
			{1L, 1000L, 1L, "/nonode", new DumbWatcher()},
		});
	}
	
	
	public TestDataTreeStats(long zxid, long time, long ephemOwner, String path, DumbWatcher dw) 
			throws NoNodeException, NodeExistsException {
		this.configure(zxid, time, ephemOwner, path, dw);
	}
	
	
	private void configure(long zxid, long time, long ephemOwner, String path, DumbWatcher dw) 
			throws NoNodeException, NodeExistsException {
		this.zxid = zxid;
		this.time = time;
		this.ephemOwner = ephemOwner;
		this.path = path;
		this.watcher = dw;
		
		this.logger = Logger.getLogger("DTS");
		this.dt = new DataTree();
		this.dt.createNode("/node", "data".getBytes(), AclParser.parse("world:1:c"), 
				1L, 1, 1L, 1000L);
	}
	
	
	@Test
	public void testStatCreation() {
		Stat stat;
		try {
			stat = this.dt.statNode(this.path, this.watcher);
			assertNotNull(stat);
		} catch (NoNodeException e) {
			this.logger.log(Level.SEVERE, "Error, cannot create Stat node \n");
		}
	}
	
	
	@Test
	public void testStatPers(){
		StatPersisted statP = DataTree.createStat(this.zxid, this.time, this.ephemOwner);
		assertEquals(statP.getEphemeralOwner(), this.ephemOwner);
	}
}
