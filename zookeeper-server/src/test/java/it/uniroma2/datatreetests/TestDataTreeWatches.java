package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DumbWatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/* Test the functionalities offered by DataTree to manage Watchers*/


@RunWith(value = Parameterized.class)
public class TestDataTreeWatches {
	private String basePath;
	private DumbWatcher watcher;
	private int mode;
	private Watcher.WatcherType type;
	
	private DataTree dt;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{"/testwatch", new DumbWatcher(), 0, Watcher.WatcherType.Any},
			{"/testwatch", new DumbWatcher(), 1, Watcher.WatcherType.Children},
			{"/testwatch", new DumbWatcher(), 1, Watcher.WatcherType.Data},
			
		});
	}
	
	
	public TestDataTreeWatches(String basePath, DumbWatcher dw, int mode, Watcher.WatcherType type) 
			throws NoNodeException, NodeExistsException {
		this.configure(basePath, dw, mode, type);	
	}
	
	
	private void configure(String basePath, DumbWatcher dw, int mode, Watcher.WatcherType type) 
			throws NoNodeException, NodeExistsException {
		this.basePath = basePath;
		this.watcher= dw;
		this.mode = mode;
		this.type = type;
		
		this.dt = new DataTree();
		dt.createNode("/testwatch", "watch".getBytes(), AclParser.parse("world:1:c"), 
				-1L, 1, 1L, 1000L);
	}
	
	
	@Test
	public void testWatcherCreationAndRemoval() {
		int before = this.dt.getWatchCount();
		this.dt.addWatch(this.basePath, this.watcher, this.mode);
		int after = this.dt.getWatchCount();
		
		assertNotEquals(before, after);
		assertTrue(this.dt.containsWatcher(this.basePath, this.type, this.watcher));
		
		this.dt.removeWatch(this.basePath, this.type, this.watcher);
		int afterRemove = this.dt.getWatchCount();
		if(this.mode == 0)
			assertEquals(before, afterRemove);
		else
			assertEquals(before+1, afterRemove);
	}
	
	
	@Test
	public void testGetWatches() {
		this.dt.addWatch(this.basePath, this.watcher, this.mode);
		assertTrue(this.dt.getWatches().getPaths(0).contains(this.basePath));
		assertEquals(1, this.dt.getWatchesByPath().getSessions(this.basePath).size());
		assertEquals(1, this.dt.getWatchesSummary().getNumPaths());
	}
	

	@Test
	public void testDumpWatches() throws FileNotFoundException {
		File out1 = new File("out1.txt");
		File out2 = new File("out2.txt");
		PrintWriter pw1 = new PrintWriter(out1);
		PrintWriter pw2 = new PrintWriter(out2);
		
		long size1Before = out1.getFreeSpace();
		long size2Before = out2.getFreeSpace();
		
		this.dt.addWatch(this.basePath, this.watcher, this.mode);
		
		this.dt.dumpWatches(pw1, true);
		this.dt.dumpWatchesSummary(pw2);
		
		long size1After = out1.getFreeSpace();
		long size2After = out2.getFreeSpace();
		
		assertEquals(size1Before, size1After);
		assertEquals(size2Before, size2After);
		
		out1.delete();
		out2.delete();
	}
	
	
	@Test
	public void testRemoveCnxn() {
		int before = this.dt.getWatchCount();
		
		this.dt.addWatch(this.basePath, this.watcher, this.mode);
		this.dt.removeCnxn(this.watcher);
		
		int after = this.dt.getWatchCount();
		
		assertEquals(before, after);
	}
	
	
	@Test
	public void testShutdown() {
		int before = this.dt.getWatchCount();
		
		this.dt.addWatch(this.basePath, this.watcher, this.mode);
		this.dt.shutdownWatcher();
		
		int after = this.dt.getWatchCount();
		
		assertNotEquals(before, after);
	}
}
