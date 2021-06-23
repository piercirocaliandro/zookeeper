package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DumbWatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/* Test to check if the data filed of the NodeTree is correctly modified */

@RunWith(value = Parameterized.class)
public class TestNodeData {
	private DataTree dt;
	
	private String path;
	private byte[] data;
	private int version;
	private long zxid;
	private long time;
	private Stat stat;
	private DumbWatcher watcher;
	
	@Parameters
	public static Collection<Object[]> getParams() {
		return Arrays.asList(new Object[][] {
			{"/datatest", "".getBytes(), 1, 10L, 1000L, new Stat(), new DumbWatcher()},
			{"/prova", "".getBytes(), 1, 10L, 1000L, new Stat(), new DumbWatcher()},
			{"", "".getBytes(), 1, 10L, 1000L, new Stat(), new DumbWatcher()},
			{null, "".getBytes(), 1, 10L, 1000L, new Stat(), new DumbWatcher()},
			{"/datatest", "aaaa".getBytes(), 1, 10L, 1000L, new Stat(), null},
			{"/datatest", "aaaa".getBytes(), -1, 10L, 1000L, null, new DumbWatcher()},
			{"/datatest", "aaaa".getBytes(), Integer.MAX_VALUE, 10L, 1000L, 
				new Stat(), new DumbWatcher()},
			{"/datatest", "aaaa".getBytes(), 1, -1L, 1000L, new Stat(), new DumbWatcher()},
			{"/datatest", "aaaa".getBytes(), -1, Long.MAX_VALUE, Long.MAX_VALUE, 
				new Stat(), new DumbWatcher()},
			{"/datatest", "aaaa".getBytes(), 1, 10L, -1L, new Stat(), new DumbWatcher()},
		});
	}
	
	
	public TestNodeData(String path, byte[] data, int version, long zxid, long time, Stat stat, 
			DumbWatcher dw) {
		this.configure(path, data, version, zxid, time, stat, dw);
	}
	
	
	private void configure(String path, byte[] data, int version, long zxid, long time, 
			Stat stat, DumbWatcher dw) {
		this.dt = new DataTree();
		try {
			this.dt.createNode("/datatest", "BBBB".getBytes(), AclParser.parse("world:1:c"), 
					-1L, 1, 1, 10000);
		} catch (NoNodeException | NodeExistsException e) {
			System.out.println("Error while adding node");
		}
		
		this.path = path;
		this.data = data;
		this.version = version;
		this.zxid = zxid;
		this.time = time;
		this.stat = stat;
		this.watcher = dw;
	}
	
	
	@Test
	public void testDataChange() throws NoNodeException {
		byte[] data = this.dt.getData(this.path, this.stat, this.watcher);
		Stat tempStat = this.dt.setData(this.path, this.data, this.version, this.zxid, this.time);
		
		assertEquals(tempStat.getDataLength(), this.data.length);
		assertNotEquals(data, this.data);
	}
}
