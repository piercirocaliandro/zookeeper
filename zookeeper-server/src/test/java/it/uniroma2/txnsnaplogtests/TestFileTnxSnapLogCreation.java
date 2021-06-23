package it.uniroma2.txnsnaplogtests;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DumbWatcher;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestFileTnxSnapLogCreation {
	private File dataDir;
	private File snapDir;
	private Request req;
	private FileTxnSnapLog txnSnapLog;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{"datadir", "snapdir", new Request(1L, 1, 0, new TxnHeader(), new Stat(), 1L)},
			{"/datadir", "snapdir", new Request(1L, 1, 0, new TxnHeader(), new Stat(), 1L)},
			{"datadir", "/snapdir", new Request(1L, 1, 0, new TxnHeader(), new Stat(), 1L)},
			{null, "snapdir", new Request(1L, 1, 0, new TxnHeader(), new Stat(), 1L)},
			{"datadir", null, new Request(1L, 1, 0, new TxnHeader(), new Stat(), 1L)},
			{"", "snapdir", new Request(1L, 1, 0, new TxnHeader(), new Stat(), 1L)},
			{"datadir", "", new Request(1L, 1, 0, new TxnHeader(), new Stat(), 1L)},
			{"datadir", "snapdir", null},
			{"datadir", "snapdir", new Request(-1L, 1, 0, new TxnHeader(), new Stat(), 1L)},
			{"datadir", "snapdir", new Request(1L, 1, 0, new TxnHeader(), new Stat(), -1L)},
		});
	}
	
	
	public TestFileTnxSnapLogCreation(String dataDir, String snapDir, Request req) 
			throws IOException {
		this.configure(dataDir, snapDir, req);
	}
	
	
	private void configure(String dataDir, String snapDir, Request req) throws IOException {
		if(dataDir != null) {
			this.dataDir = new File(dataDir);
			this.dataDir.mkdir();
		}
		if(snapDir != null) {
			this.snapDir = new File(snapDir);
			this.snapDir.mkdir();
		}
		
		this.req = req;
		this.txnSnapLog = new FileTxnSnapLog(this.dataDir, this.snapDir);
	}
	
	
	@Test
	public void testAppend() throws IOException {
		assertTrue(this.txnSnapLog.append(this.req));
		
		this.txnSnapLog.close();
	}
}
