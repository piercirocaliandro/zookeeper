package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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
import org.apache.zookeeper.server.DataNode;
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
	private DataNode dn;
	
	
	@Parameters
	public static Collection<Object[]> getParams() {
		return Arrays.asList(new Object[][] {
			{"/valuetest", "world:10:c", 1, new Stat(), 
				new DataNode("AAAA".getBytes(), 1L, new StatPersisted())},
			{"", "world:10:c", 1, new Stat(), 
					new DataNode("AAAA".getBytes(), 1L, new StatPersisted())},
			{"/valuetest", "world:10:c", -11, new Stat(), 
				new DataNode("AAAA".getBytes(), 1L, null)},
			{"/valuetest", "dvw:1:h", 1, new Stat(), 
				new DataNode("".getBytes(), 1L, new StatPersisted())},
			
			//added to increase coverage
			{"/nonode", "world:10:c", -11, new Stat(), 
				new DataNode("AAAA".getBytes(), -1L, new StatPersisted())},
			{"/valuetest", "world:10:c", -11, null, 
				new DataNode("AAAA".getBytes(), 1L, new StatPersisted())},
		});
	}
	
	
	public TestDataTreeNodeACL(String path, String acls, int version, Stat stat, DataNode dn) {
		this.configure(path, acls, version, stat, dn);
	}
	
	
	private void configure(String path, String acls, int version, Stat stat, DataNode dn) {
		this.path = path;
		this.acls = acls;
		this.version = version;
		this.stat = stat;
		this.dn = dn;
		
		this.dt = new DataTree();
		try {
			this.dt.createNode("/valuetest", "abcde".getBytes(), AclParser.parse("world:10:r"), 
					2L, 1, 10L, 100000L);
		} catch (NoNodeException | NodeExistsException e) {
			Logger.getLogger("DNF").log(Level.SEVERE, "Failed to create the node");
		}
	}
	
	
	@Test
	public void testAclGet() {
		try {
			assertNotEquals(0, this.dt.getACL(this.path, this.stat).size());
		} catch (NoNodeException e) {
			Logger.getLogger("TACL").log(Level.WARNING, "Failed to set ACL\n");
		}
	}
	
	
	@Test
	public void testGetAclWithNode() {
		assertEquals(1, this.dt.getACL(this.dn).size());
	}
	
	
	@Test
	public void testAclSet() {
		try {
			Stat stat = this.dt.setACL(this.path, AclParser.parse(this.acls), this.version);
			assertNotNull(stat);
		} catch (NoNodeException e) {
			Logger.getLogger("TACL").log(Level.WARNING, "Failed to get ACL\n");
		}
	}
	
	
	@Test
	public void testAclCacheSize() {
		assertEquals(this.dt.getReferenceCountedAclCache().size(), 
				this.dt.aclCacheSize());
	}
}
