package it.uniroma2.datatreetests;

import static org.junit.Assert.assertNotEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DumbWatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import it.uniroma2.datatree.utils.DataTreeTestCommon;

/* Test only the setWatches() method, due to it gets a lot of input parameters */


@RunWith(value = Parameterized.class)
public class TestDataTreeSetWatches extends DataTreeTestCommon{
	private long relativeZxid;
	private List<String> dataWatches;
	private List<String> existsWatches;
	private List<String> childWatches;
	private List<String> persistentWatches;
	private List<String> persistentRecursiveWatches;
	private DumbWatcher dw;
	private DataTree dt;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{1L, Arrays.asList("/pierapp", "/nonode1"), Arrays.asList("/pierapp1", "/nonode2"),
				Arrays.asList("/pierapp/app1", "/nonode3"), Arrays.asList("/pierapp/app2", "/nonode4"),
				Arrays.asList("/pierapp1/app3", "/nonode5"), new DumbWatcher()},
			{Long.MIN_VALUE, Arrays.asList("/pierapp", "/nonode1"), 
				Arrays.asList("/pierapp1", "/nonode2"), Arrays.asList("/pierapp/app1", "/nonode3"),
				Arrays.asList("/pierapp/app2", "/nonode4"), 
				Arrays.asList("/pierapp1/app3", "/nonode5"), new DumbWatcher()},
			
		});
	}
	
	
	public TestDataTreeSetWatches(long relativeZxid, List<String> dataWatches, 
			List<String> existsWatches, List<String> childWatches, 
			List<String> persistentWatches, List<String> persistentRecursiveWatches, DumbWatcher dw) {
		this.configure(relativeZxid, dataWatches, existsWatches, 
				childWatches, persistentWatches, persistentRecursiveWatches, dw);
	}
	
	
	private void configure(long relativeZxid, List<String> dataWatches, 
			List<String> existsWatches, List<String> childWatches, 
			List<String> persistentWatches, List<String> persistentRecursiveWatches, DumbWatcher dw) {
		
		this.relativeZxid = relativeZxid;
		this.dataWatches = dataWatches;
		this.existsWatches = existsWatches;
		this.childWatches = childWatches;
		this.persistentWatches = persistentWatches;
		this.persistentRecursiveWatches = persistentRecursiveWatches;
		this.dw = dw;
		
		this.dt = new DataTree();
		this.createNodes(this.getNodes(), this.dt);
	}
	
	
	@Test
	public void testSetWatches() {
		int before = this.dt.getWatchCount();
		this.dt.setWatches(this.relativeZxid, this.dataWatches, this.existsWatches, 
				this.childWatches, this.persistentWatches, this.persistentRecursiveWatches, this.dw);
		int after = this.dt.getWatchCount();
		
		assertNotEquals(before, after);
	}
}
