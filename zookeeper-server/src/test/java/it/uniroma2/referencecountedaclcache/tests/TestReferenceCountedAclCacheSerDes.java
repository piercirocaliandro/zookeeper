package it.uniroma2.referencecountedaclcache.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.ReferenceCountedACLCache;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestReferenceCountedAclCacheSerDes {
	private BinaryInputArchive ia;
	private BinaryOutputArchive oa;
	private ReferenceCountedACLCache rcAclCache;
	
	
	@Parameters
	public static Collection<Object[]> getParams() throws FileNotFoundException{
		return Arrays.asList(new Object[][] {
			{new BinaryInputArchive(new DataInputStream(new FileInputStream(new File("in")))), 
				new BinaryOutputArchive(new DataOutputStream(new FileOutputStream(new File("out"))))},
			{null, null}
		});
	}
	
	
	public TestReferenceCountedAclCacheSerDes(BinaryInputArchive ia, BinaryOutputArchive oa) {
		this.configure(ia, oa);
	}
	
	
	private void configure(BinaryInputArchive ia, BinaryOutputArchive oa) {
		this.ia = ia;
		this.oa = oa;
		this.rcAclCache = new ReferenceCountedACLCache();
	}
	
	
	@Test
	public void testAclsSer() throws IOException {
		this.rcAclCache.serialize(this.oa);
	}
}
