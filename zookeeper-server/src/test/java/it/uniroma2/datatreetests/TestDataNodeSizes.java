package it.uniroma2.datatreetests;

import static org.junit.Assert.assertNotEquals;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.server.DataTree;
import org.junit.Before;
import org.junit.Test;

import it.uniroma2.datatree.utils.DataTreeTestCommon;

/* Test the method that return the sizes of DataNodes*/

public class TestDataNodeSizes extends DataTreeTestCommon{
	private DataTree dt;
	
	
	@Before
	public void configure() {
		this.dt = new DataTree();
		this.createNodes(this.getNodes(), this.dt);
	}
	
	
	@Test
	public void testApproxDataSize() throws NoNodeException, NodeExistsException {
		long before = this.dt.approximateDataSize();
		long cachedBefore = this.dt.cachedApproximateDataSize();
		
		this.dt.createNode("/testnode", "aaaa".getBytes(), 
				AclParser.parse("world:10:w"), -1L, 10, 1L, 10L);
		long after = this.dt.approximateDataSize();
		long cachedAfter = this.dt.cachedApproximateDataSize();
		
		assertNotEquals(before, after);
		assertNotEquals(cachedBefore, cachedAfter);
		
		this.dt.deleteNode("/testnode", 1L);
	}
}
