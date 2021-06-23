package it.uniroma2.datatreetests;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.server.DataTree;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/* Test serialization/deserialization */

@RunWith(value = Parameterized.class)
public class TestDataTreeSerDes {
	private BinaryInputArchive ia;
	private BinaryOutputArchive oa;
	private String tag;
	
	private DataTree dt;
	private static File in;
	
	
	@Parameters
	public static Collection<Object[]> getParams() throws IOException{
		in = new File("prova.txt");
		in.createNewFile();
		
		return Arrays.asList(new Object[][] {
			{new BinaryInputArchive(new DataInputStream(new 
					FileInputStream(in))), 
				new BinaryOutputArchive(new DataOutputStream(
						new FileOutputStream(in))), "TAG"},
			
			/*{new BinaryInputArchive(new DataInputStream(new 
					FileInputStream(in))), 
				new BinaryOutputArchive(new DataOutputStream(
						new FileOutputStream(in))), ""},
			
			{new BinaryInputArchive(new DataInputStream(new 
					FileInputStream(in))), 
				new BinaryOutputArchive(new DataOutputStream(
						new FileOutputStream(in))), null},
			
			{null, null, "TAG"}*/
		});
	}
	
	
	public TestDataTreeSerDes(BinaryInputArchive ia, BinaryOutputArchive oa, String tag) 
			throws NoNodeException, NodeExistsException {
		this.ia = ia;
		this.oa = oa;
		this.tag = tag;
		
		this.dt = new DataTree();
		this.dt.createNode("/node1", "data".getBytes(), AclParser.parse("world:1:c"), 
				1L, 1, 1L, 1000L);
	}
	
	
	@Test
	public void testSerDes() throws IOException, NoNodeException {
		long before = in.length();
		this.dt.serialize(this.oa, this.tag);
		long after = in.length();
		assertNotEquals(before, after);
		
		this.dt.deleteNode("/node1", 1L);
		this.dt.deserialize(this.ia, this.tag);
		
		assertNotNull(this.dt.getNode("/node1"));
	}
}
