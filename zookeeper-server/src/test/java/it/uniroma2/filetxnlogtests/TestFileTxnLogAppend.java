package it.uniroma2.filetxnlogtests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.jute.Record;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(value = Parameterized.class)
public class TestFileTxnLogAppend {
	private TxnHeader header;
	private Record record;
	private TxnDigest digest;
	
	private File logDir;
	private FileTxnLog txnLog;
	private Logger logger;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{new TxnHeader(1L, 0, 1L, 1000L, 1), 
				new CreateTxn("/test1", "AAAA".getBytes(), null, false, 0), 
				new TxnDigest()},
			{new TxnHeader(1L, 0, 1L, 1000L, 1), 
				new CreateTxn("/test1", "AAAA".getBytes(), null, false, 0),
				new TxnDigest()},
			{null, 
				new CreateTxn("/test1", "AAAA".getBytes(), null, false, 0),
				new TxnDigest()},
			{new TxnHeader(1L, 0, 1L, 1000L, 1), 
				null, new TxnDigest()},
			{new TxnHeader(1L, 0, 1L, 1000L, 1), 
				new CreateTxn("/test1", "AAAA".getBytes(), null, false, 0), 
				new TxnDigest()},
			
			{new TxnHeader(1L, 0, Long.MIN_VALUE, 1000L, 1), 
				new CreateTxn("/test1", "AAAA".getBytes(), null, false, 0), 
				new TxnDigest()},
		});
	}
	
	
	public TestFileTxnLogAppend(TxnHeader header, Record record, TxnDigest digest) 
			throws IOException {
		this.configure(header, record, digest);
	}
	
	
	private void configure(TxnHeader header, Record record, TxnDigest digest) throws IOException {
		this.header = header;
		this.record = record;
		this.digest = digest;
		
		this.logDir = new File("logdir");
		this.logDir.mkdir();
		
		this.txnLog = new FileTxnLog(this.logDir);
		
		this.logger = Logger.getLogger("TXN"); 
	}
	
	
	@After
	public void cleanEnv() {
		try {
			if(this.logDir != null)
				FileUtils.deleteDirectory(this.logDir);
			
			if(this.header != null) {
				File f = new File("log."+this.header.getClientId());
				f.delete();
			}
		} catch (IOException e) {
			this.logger.log(Level.WARNING, "Failed to delete directory\n");
		}
	}
	
	
	@Test
	public void testAppend() throws IOException {
		if(this.header == null)
			assertFalse(this.txnLog.append(this.header, this.record));
		else {
			assertTrue(this.txnLog.append(this.header, this.record));
		}
		this.txnLog.close();
	}
	
	
	@Test
	public void testAppendWithDigest() throws IOException {
		if(this.header == null)
			assertFalse(this.txnLog.append(this.header, this.record, this.digest));
		else {
			assertTrue(this.txnLog.append(this.header, this.record, this.digest));
		}
		this.txnLog.close();
	}
}
