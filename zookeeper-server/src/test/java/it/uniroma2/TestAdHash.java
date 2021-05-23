package it.uniroma2;

import static org.junit.Assert.assertEquals;

import org.apache.zookeeper.server.util.AdHash;
import org.junit.Test;

public class TestAdHash {
	
	@Test
	public void testAdHash() {
		AdHash ah = new AdHash();
		ah.addDigest(1234);
		assertEquals(1234, ah.getHash(), 0.0);
	}
}
