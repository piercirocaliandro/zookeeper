package it.uniroma2.filetxnlog.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.TxnLog;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/* Test the read from a log */


@RunWith(value = Parameterized.class)
public class TestFileTxnLogRead {
	private long zxid;
	private boolean fastForward;
	
	private FileTxnLog fTxnLog;
	private File logDir;
	private Logger logger;
	private long nEntries = 3L;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{0L, true},
			{1L, true},
			{2L, true},
			{1L, false},
			{-1L, true},
			//{Long.MAX_VALUE, true},
		});
	}
	
	
	public TestFileTxnLogRead(long zxid, boolean fastForward) throws IOException {
		this.configure(zxid, fastForward);
	}
	
	
	private void configure(long zxid, boolean fastForward) throws IOException {
		this.logDir = new File("logdir");
		this.logDir.mkdir();
		
		this.zxid = zxid;
		this.fastForward = fastForward;
		
		this.fTxnLog = new FileTxnLog(this.logDir);
		this.logger = Logger.getLogger("TXNR");
		
		Map<TxnHeader, CreateTxn> entries = this.getLogEntries(this.nEntries);
		
		for(TxnHeader header : entries.keySet()) {
			this.fTxnLog.append(header, entries.get(header));
			this.fTxnLog.commit();
		}
	}
	
	
	@After
	public void cleanEnv() {
		try {
			FileUtils.deleteDirectory(this.logDir);
			this.fTxnLog.close();
		} catch (IOException e) {
			this.logger.log(Level.WARNING, "Error while cleaning env \n");
		}
	}
	
	
	/* Return a Map of Headers and Records, to initialize log */
	private Map<TxnHeader, CreateTxn> getLogEntries(long cap){
		Map<TxnHeader, CreateTxn> entryMap = new HashMap<>();
		for(long zxid = 0; zxid < cap; zxid++) {
			TxnHeader header = new TxnHeader(1L, 0, zxid, 1000L, 1);
			CreateTxn txn = new CreateTxn("/test1", "AAAA".getBytes(), AclParser.parse("world:1:c"), 
					false, 0);
			entryMap.put(header, txn);
		}
		
		return entryMap;
	}
	
	
	@Test
	public void testRead() throws IOException {
		TxnLog.TxnIterator iterator = this.fTxnLog.read(this.zxid);
		TxnLog.TxnIterator iterator2 = this.fTxnLog.read(this.zxid, this.fastForward);
		
		for(long i = 0; i < this.nEntries-1; i++) {
			assertTrue(iterator.next());
			assertTrue(iterator2.next());
		}
		assertFalse(iterator.next());
		assertFalse(iterator2.next());
		
		// close resource
		iterator.close();
		iterator2.close();
	}
	
	
	@Test
	public void testRoll() throws IOException {
		long prevRoll = this.fTxnLog.getTotalLogSize();
		this.fTxnLog.rollLog();
		long aftRoll = this.fTxnLog.getTotalLogSize();
		
		assertNotEquals(prevRoll, aftRoll);
	}
}
