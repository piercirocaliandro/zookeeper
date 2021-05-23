package it.uniroma2.zookeeper.electiontests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.zookeeper.recipes.leader.LeaderOffer;
import org.apache.zookeeper.recipes.leader.LeaderOffer.IdComparator;
import org.junit.Test;

public class LeaderOfferTest {
	
	@Test
	public void testId() {
		LeaderOffer lo = new LeaderOffer(12, "ciao", "pippo");
		assertEquals(12, lo.getId(), 0.0);
	}
	
	@Test
	public void testNodePath() {
		LeaderOffer lo = new LeaderOffer();
		lo.setNodePath("prova");
		assertEquals("prova", lo.getNodePath());
	}
	
	@Test
	public void testHostName() {
		LeaderOffer lo = new LeaderOffer();
		lo.setHostName("localhost");
		assertEquals("localhost", lo.getHostName());
	}
	
	@Test
	public void testComparator() {
		IdComparator idComp = new LeaderOffer.IdComparator();
		LeaderOffer o1 = new LeaderOffer(12, "/node1", "localhost");
		LeaderOffer o2 = new LeaderOffer(13, "/node2", "localhost");
		assertNotEquals(0, idComp.compare(o1, o2));
	}
}
