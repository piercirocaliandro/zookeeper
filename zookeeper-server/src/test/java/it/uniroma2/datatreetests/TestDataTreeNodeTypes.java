package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.server.DataTree;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/* Test getters for various types of nodes */

public class TestDataTreeNodeTypes {
	private DataTree dt;
	private int nBefore;
	private int nAfter;
	
	
	@Before
	public void configure() {
		
		// allow TTL nodes creation
		System.setProperty("zookeeper.extendedTypesEnabled", "true");
		
		this.dt = new DataTree();
	}
	
	
	@After
	public void cleanEnv() {
		System.setProperty("zookeeper.extendedTypesEnabled", "false");
	}
	
	
	@Test
	public void testSessions() throws NoNodeException, NodeExistsException {
		this.nBefore = this.dt.getSessions().size();
		this.dt.createNode("/ephem", "AAAA".getBytes(), AclParser.parse("world:1:c"), 
				2L, 1, 1, 100L);
		this.nAfter = this.dt.getSessions().size();
		assertEquals(this.nBefore+1, this.nAfter);
	}
	
	
	@Test
	public void testContainers() throws NoNodeException, NodeExistsException {
		this.nBefore = this.dt.getContainers().size();
		this.dt.createNode("/cont", "AAAA".getBytes(), AclParser.parse("world:1:c"), 
				Long.MIN_VALUE, 1, 1, 100L);
		this.nAfter = this.dt.getContainers().size();
		assertEquals(this.nBefore+1, this.nAfter);
	}
	
	
	@Test
	public void testTTLs() throws NoNodeException, NodeExistsException {
		this.nBefore = this.dt.getContainers().size();
		this.dt.createNode("/ttl", "AAAA".getBytes(), AclParser.parse("world:1:c"), 
				new BigInteger("ff00000000000100", 16).longValue(), 1, 1, 100L);
		this.nAfter = this.dt.getTtls().size();
		assertEquals(this.nBefore+1, this.nAfter);
	}
}
