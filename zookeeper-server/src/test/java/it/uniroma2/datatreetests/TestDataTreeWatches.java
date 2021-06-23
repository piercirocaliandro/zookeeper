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
			{"", new DumbWatcher(), 1},
			{"/testwatch", new DumbWatcher(), 0, Watcher.WatcherType.Any},
			{"/testwatch", new DumbWatcher(), 1, Watcher.WatcherType.Children},
			{"/testwatch", new DumbWatcher(), 1, Watcher.WatcherType.Data},
			{"/testwatch", new DumbWatcher(), 2, Watcher.WatcherType.Any},
			{"/testwatch", new DumbWatcher(), -1, Watcher.WatcherType.Any},
			{"/testwatch", new DumbWatcher(), Integer.MAX_VALUE, Watcher.WatcherType.Any},
			{"/testwatch", null, 1, Watcher.WatcherType.Any},
			{null, new DumbWatcher(), 1, Watcher.WatcherType.Any},
			{"/nonode", new DumbWatcher(), 1, Watcher.WatcherType.Any},
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
	
	
	/* This won't produce anything, due to the watcher is a dummy implemenation
	 * therefore it as no content to be printed (maybe)
	 * */
	@Test
	public void testDumpWatches() throws FileNotFoundException {
		File out1 = new File("/home/pierciro/Scrivania/isw2-folder/out1.txt");
		File out2 = new File("/home/pierciro/Scrivania/isw2-folder/out2.txt");
		PrintWriter pw1 = new PrintWriter(out1);
		PrintWriter pw2 = new PrintWriter(out2);
		
		this.dt.dumpWatches(pw1, true);
		this.dt.dumpWatchesSummary(pw2);
	}
	
	
	@Test
	public void testGetWatches() {
		this.dt.addWatch(this.basePath, this.watcher, this.mode);
		assertTrue(this.dt.getWatches().getPaths(0).contains(this.basePath));
		assertEquals(1, this.dt.getWatchesByPath().getSessions("/testwatch").size());
		assertEquals(1, this.dt.getWatchesSummary().getNumPaths());
	}
}
