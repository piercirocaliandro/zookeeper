package it.uniroma2.filetxnlog.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(value = Parameterized.class)
public class TestFileTxnZxid {
	private long zxid;
	private File[] logDirList;
	
	private FileTxnLog txnLog;
	private Logger logger;
	private File logdir;
	private String logDirPath = "logdir";
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{1L, Arrays.asList(new File("log.-1"), new File("log.0"),
					new File("log.1"))},
			//{0L, Arrays.asList(new File("log.0"))},
			//{-1L, Arrays.asList(new File("log.-1, log.1, log.2"))},
			{1L, Arrays.asList()},
			//{Long.MIN_VALUE, Arrays.asList(new File("log.1"))},
			//{1L, null},
		});
	}
	
	
	public TestFileTxnZxid(long zxid, List<File> logDirList) throws IOException {
		this.configure(zxid, logDirList);
	}
	
	
	private void configure(long zxid, List<File> logDirList) throws IOException {
		this.zxid = zxid;
		if(logDirList != null)
			this.logDirList = logDirList.toArray(new File[logDirList.size()]);
		else
			this.logDirList= null;
		
		this.logdir = new File(this.logDirPath);
		this.logdir.mkdir();
		this.txnLog = new FileTxnLog(this.logdir);
		
		this.logger = Logger.getLogger("FXNZ");
		
		// append a new record in the log
		for(long id = 0; id < 2; id++) {
			this.txnLog.append(new TxnHeader(1L, 0, id, 1000L, 1), 
					new CreateTxn("/test1", "".getBytes(), null, false, 0));
			this.txnLog.commit();
		}
	}
	
	
	@After
	public void cleanEnv() {
		try {
			FileUtils.deleteDirectory(this.logdir);
			this.txnLog.close();
		} catch (IOException e) {
			this.logger.log(Level.WARNING, "Failed to clean env\n");
		}
	}
	
	
	@Test
	public void testLastLoggedZxid() {
		assertEquals(this.zxid, this.txnLog.getLastLoggedZxid());
	}
	
	
	@Test
	public void testGetLogFiles() {
		File[] logs = FileTxnLog.getLogFiles(this.logDirList, this.zxid);
		if(this.zxid < 0 || this.logDirList == null || this.logDirList.length == 0)
			assertEquals(0, logs.length);
		else
			assertEquals(1, logs.length);
	}
	
	
	@Test
	public void testLogTrunc() {
		long beforeTrunc = this.txnLog.getTotalLogSize();
		try {
			assertTrue(this.txnLog.truncate(this.zxid));
			assertTrue(beforeTrunc > this.txnLog.getTotalLogSize());
		} catch (IOException e) {
			this.logger.log(Level.WARNING, "Fail to truncate log with zxid:"+this.zxid);
		}
	}
}
