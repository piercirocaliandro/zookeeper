package it.uniroma2.filetxnlog.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/* Test some methods, just to increase statement and branch coverage metrics */

public class TestFileTxnLogDbAndSync {
	private FileTxnLog txnLog;
	private Logger logger;
	private File logdir;
	private String logDirPath = "logdir";

	
	@Before
	public void configure() {
		this.logdir = new File(this.logDirPath);
		this.logdir.mkdir();
		this.txnLog = new FileTxnLog(this.logdir);
		
		this.logger = Logger.getLogger("FXNZ");
	}
	
	
	@After
	public void cleanEnv() {
		try {
			FileUtils.deleteDirectory(this.logdir);
			this.txnLog.close();
			System.clearProperty("zookeeper.forceSync");
		} catch (IOException e) {
			this.logger.log(Level.WARNING, "Failed to clean env\n");
		}
	}
	
	
	@Test
	public void testGetDb() throws IOException {
		this.txnLog.append(new TxnHeader(1L, 0, 1L, 1000L, 1), 
				new CreateTxn("/test1", "AAAA".getBytes(), null, false, 0));
		this.txnLog.commit();
		
		assertEquals(0, this.txnLog.getDbId(), 0.0);
	}
	
	
	@Test
	public void testIsSync() {
		System.setProperty("zookeeper.forceSync", "yes");
		assertTrue(this.txnLog.isForceSync());
	}
	
	
	@Test
	public void getElapsedTime() {
		assertTrue(this.txnLog.getTxnLogSyncElapsedTime() < 0L);
	}
}
