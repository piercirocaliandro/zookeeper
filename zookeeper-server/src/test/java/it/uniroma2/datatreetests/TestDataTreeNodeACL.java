package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/* Test changes to the data and ACL fields of a given node */

@RunWith(value = Parameterized.class)
public class TestDataTreeNodeACL {
	private DataTree dt;
	
	private String path;
	private String acls;
	private int version;
	private Stat stat;
	
	
	@Parameters
	public static Collection<Object[]> getParams() {
		return Arrays.asList(new Object[][] {
			{"/valuetest", "world:10:c", 1, new Stat()},
			{"", "world:10:c", 1, new Stat()},
			{"/valuetest", "world:10:c", -11, new Stat()},
			{"/valuetest", "dvw:1:h", 1, new Stat()},
		});
	}
	
	
	public TestDataTreeNodeACL(String path, String acls, int version, Stat stat) {
		this.configure(path, acls, version, stat);
	}
	
	
	private void configure(String path, String acls, int version, Stat stat) {
		this.path = path;
		this.acls = acls;
		this.version = version;
		this.stat = stat;
		
		this.dt = new DataTree();
		try {
			this.dt.createNode("/valuetest", "abcde".getBytes(), AclParser.parse("world:10:r"), 
					2L, 1, 10L, 100000L);
		} catch (NoNodeException | NodeExistsException e) {
			Logger.getLogger("DNF").log(Level.SEVERE, "Failed to create the node");
		}
	}
	
	
	@Test
	public void testAclModify() throws NoNodeException {
		this.dt.getACL(this.dt.getNode(this.path));
		this.dt.statNode(this.path, null).getAversion();
		Stat stat = this.dt.setACL(this.path, AclParser.parse(this.acls), this.version);
		this.dt.getACL(this.path, this.stat);
		
		assertEquals(stat, this.stat);
	}
	
	
	@Test
	public void testAclCacheSize() {
		assertEquals(this.dt.getReferenceCountedAclCache().size(), 
				this.dt.aclCacheSize());
	}
}
