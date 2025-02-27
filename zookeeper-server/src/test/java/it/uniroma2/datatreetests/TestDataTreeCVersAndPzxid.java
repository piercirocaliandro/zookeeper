package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DumbWatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestDataTreeCVersAndPzxid {
	private String path;
	private int newCVers;
	private long zxid;
	
	private DataTree dt;
	private Logger logger;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{"/app1", 3, 1L},
			{"", 3, 1L}, 
			{"/", 30, 1L},
			{"/app1", -1, 1L},
			{"/app1", 30, 1L},
			{"/app1", Integer.MAX_VALUE, 1L},
			{"/app1", 30, -1L},
			{"/app1", 30, Long.MAX_VALUE},
			{"/app", 1, Long.MAX_VALUE},
			
			{"/app1", Integer.MIN_VALUE, 1L},
		});
	}
	
	
	public TestDataTreeCVersAndPzxid(String path, int newCVers, long zxid) 
			throws NoNodeException, NodeExistsException {
		this.configure(path, newCVers, zxid);
	}
	
	
	private void configure(String path, int newCVers, long zxid) 
			throws NoNodeException, NodeExistsException {
		this.path = path;
		this.newCVers = newCVers;
		this.zxid = zxid;
		
		this.dt = new DataTree();
		this.dt.createNode("/app1", "abcd".getBytes(), AclParser.parse("world:1:c"), 
				1L, 1, 1L, 1000L);
		this.logger = Logger.getLogger("TDT");
	}
	
	
	@Test
	public void testCVersPzxid() {
		try {
			this.dt.setCversionPzxid(this.path, this.newCVers, this.zxid);
			assertEquals(this.zxid, this.dt.statNode(this.path, new DumbWatcher()).getPzxid());
		} catch (NoNodeException e) {
			this.logger.log(Level.SEVERE, "Failed to update cvers and zxid\n");
		};
	}
}
