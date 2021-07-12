package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.server.DataTree;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import it.uniroma2.datatree.utils.DataTreeNodeBean;
import it.uniroma2.datatree.utils.DataTreeTestCommon;

/* Test only a single method for ephemeral nodes on the boundaries of the input domain*/

@RunWith(value = Parameterized.class)
public class TestDataTreeEphemNodesBoundary extends DataTreeTestCommon{
	private long sessionId;
	private DataTree dt;
	private List<DataTreeNodeBean> nodeList;
	
	
	@Parameters
	public static Collection<Long> getParams(){
		return Arrays.asList(new Long[] {
			Long.MAX_VALUE,
			-1L,
			2L
		});
	}
	
	
	public TestDataTreeEphemNodesBoundary(long sessionId) {
		this.configure(sessionId);
	}
	
	
	private void configure(long sessionId) {
		this.dt = new DataTree();
		this.nodeList = this.getNodes();
		this.createNodes(this.nodeList, this.dt);
		
		this.sessionId = sessionId;
	}
	
	
	@Test
	public void testEphemNodesBoundaries() {
		Set<String> ephems = this.dt.getEphemerals(this.sessionId);
		int ephemSize2 = 0;
		
		for(DataTreeNodeBean dtb : this.nodeList) {
			if(dtb.getEphemeralOwner() == this.sessionId)
				ephemSize2+=1;
		}
		
		assertEquals(ephemSize2, ephems.size(), 0.0);
	}
}
