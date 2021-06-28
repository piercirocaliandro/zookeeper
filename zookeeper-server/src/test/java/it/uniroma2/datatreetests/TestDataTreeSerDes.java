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
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/* Test serialization/deserialization */

@RunWith(value = Parameterized.class)
public class TestDataTreeSerDes {
	private File file;
	private String tag;
	
	private BinaryInputArchive ia;
	private BinaryOutputArchive oa;
	private DataTree dt;
	
	
	@Parameters
	public static Collection<Object[]> getParams() throws IOException{
		return Arrays.asList(new Object[][] {
			{new File("file.txt"), "TAG"},
			{new File("file.txt"), ""},
			{new File("file.txt"), null},
			//{null, "TAG"}
		});
	}
	
	
	public TestDataTreeSerDes(File file, String tag) 
			throws NoNodeException, NodeExistsException, IOException {
		
		if(file != null) {
			this.file = file;
			this.file.createNewFile();
		
			this.ia = new BinaryInputArchive(new DataInputStream(new 
					FileInputStream(file)));
			this.oa = new BinaryOutputArchive(new DataOutputStream(
					new FileOutputStream(file)));
		}
		else {
			this.ia = null;
			this.oa = null;
		}
		
		this.tag = tag;
		
		this.dt = new DataTree();
		this.dt.createNode("/node1", "data".getBytes(), AclParser.parse("world:1:c"), 
				1L, 1, 1L, 1000L);
	}
	
	
	@After
	public void cleanEnv() {
		this.file.delete();
	}
	
	
	@Test
	public void testSerDes() throws IOException, NoNodeException {
		long before = file.length();
		this.dt.serialize(this.oa, this.tag);
		long after = file.length();
		assertNotEquals(before, after);
		
		this.dt.deleteNode("/node1", 1L);
		this.dt.deserialize(this.ia, this.tag);
		
		assertNotNull(this.dt.getNode("/node1"));
	}
}
