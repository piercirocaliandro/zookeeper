package it.uniroma2.txnsnaplogtests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestFileTxnSnapLogSnaps {
	private DataTree dt;
	private TxnHeader header;
	private  Map<Long,Integer> sessions;
	private ConcurrentHashMap<Long, Integer> sessWithTo;
	private Stat txn;
	private int n;
	private boolean syncSnap;
	
	private FileTxnSnapLog snapLog;
	private File logdir;
	private File snapDir;
	
	
	/* Get an instance of an HashMap*/
	private static Map<Long, Integer> getMap(boolean isValid){
		Map<Long, Integer> hashMap = new HashMap<>();
		if(isValid)
			hashMap.put(1L, 1000);
		else
			hashMap.put(1L, -1000);
		return hashMap;
	}
	
	
	/* Get an instance of an HashMap*/
	private static ConcurrentHashMap<Long, Integer> getCMap(boolean isValid){
		ConcurrentHashMap<Long, Integer> hashMap = new ConcurrentHashMap<>();
		if(isValid)
			hashMap.put(1L, 1000);
		else
			hashMap.put(1L, -1000);
		return hashMap;
	}
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{new DataTree(), new TxnHeader(), getMap(true), new Stat(), getCMap(true), 1, true},
			{null, new TxnHeader(), getMap(true), new Stat(), getCMap(true), 0, true},
			{new DataTree(), null, getMap(true), new Stat(), getCMap(true), 0, true},
			{new DataTree(), new TxnHeader(), getMap(false), new Stat(), getCMap(true), -1, true},
			{new DataTree(), new TxnHeader(), getMap(true), null, getCMap(true), Integer.MAX_VALUE
				, true},
			{new DataTree(), new TxnHeader(), getMap(true), new Stat(), getCMap(false), 1, false},
			{new DataTree(), new TxnHeader(), null, new Stat(), getCMap(true), 1, false},
			{new DataTree(), new TxnHeader(), getMap(true), new Stat(), null, 1, false},
		});
	}
	
	
	public TestFileTxnSnapLogSnaps(DataTree dt, TxnHeader header, Map<Long, Integer> sessions, 
			Stat stat, ConcurrentHashMap<Long, Integer> cMap, int n, boolean syncSnap) 
					throws IOException {
		BasicConfigurator.configure();
		
		this.dt = dt;
		this.header = header;
		this.sessions = sessions;
		this.sessWithTo = cMap;
		this.txn = stat;
		this.n = n;
		this.syncSnap = syncSnap;
		
		// default creation 
		this.logdir = new File("logdir");
		this.logdir.mkdir();
		this.snapDir = new File("snapdir");
		this.snapDir.mkdir();
		
		this.snapLog = new FileTxnSnapLog(this.logdir, this.snapDir);
		
	}
	
	
	@Test
	public void testTransaction() throws NoNodeException, IOException {
		this.snapLog.processTransaction(this.header, this.dt, this.sessions, this.txn);
		assertEquals(this.header.getZxid(), this.dt.lastProcessedZxid);
	}
	
	
	@Test
	public void testSnapshot() throws IOException {
		this.snapLog.save(this.dt, this.sessWithTo, this.syncSnap);
		
		assertEquals(n, this.snapLog.findNRecentSnapshots(n).size());
		assertNotNull(this.snapLog.findMostRecentSnapshot());
	}
}
