package it.uniroma2.filetxnlogtests;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
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
public class TestFileTnxLogTruncate {
	private long zxid;
	
	private File logDir;
	private Logger logger;
	private FileTxnLog txnLog;
	
	
	@Parameters
	public static Collection<Long> getParams(){
		return Arrays.asList(new Long[] {
			1L,
			-1L,
			Long.MAX_VALUE,
		});
	}
	
	
	public TestFileTnxLogTruncate(Long zxid) 
			throws IOException {
		this.configure(zxid);
	}
	
	
	private void configure(Long zxid) throws IOException {
		this.zxid = zxid;
		
		this.logDir = new File("logdir");
		this.logDir.mkdir();
		this.logger = Logger.getLogger("TXN"); 
		
		this.txnLog = new FileTxnLog(this.logDir);
		
		// append a new record in the log
		for(long id = 0; id < 2; id++) {
			this.txnLog.append(new TxnHeader(id, 0, id, 1000L, 1), 
					new CreateTxn("/test1", "".getBytes(), null, false, 0));
			this.txnLog.commit();
		}
	}
	
	
	@After
	public void cleanEnv() {
		try {
			if(this.logDir != null)
				FileUtils.deleteDirectory(this.logDir);
		} catch (IOException e) {
			this.logger.log(Level.SEVERE, "Failed to delete directory\n");
		}
	}
	
	
	@Test
	public void truncate() {
		try {
			assertTrue(this.txnLog.truncate(this.zxid));
		} catch (IOException e) {
			this.logger.log(Level.SEVERE, "An error occurred during log truncation");
		}
	}
}
