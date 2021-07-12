package it.uniroma2.filetxnlogtests;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/* Test the methods regarding with sizes */


@RunWith(value = Parameterized.class)
public class TestFileTxnLogCurrentSize {
	private long maxSize;
	
	private FileTxnLog log;
	private File dir;
	private Logger logger;
	private TxnHeader header;
	private CreateTxn crTxn;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{-1L, true},
			{-1L, false},
			{10L, true},
			{Long.MAX_VALUE, true}
		});
	}
	
	
	public TestFileTxnLogCurrentSize(long maxSize, boolean forceSync) {
		this.configure(maxSize, forceSync);
	}
	
	
	private void configure(long maxSize, boolean forceSync) {
		this.maxSize = maxSize;
		
		this.dir = new File("logdir");
		this.dir.mkdir();
		
		this.log = new FileTxnLog(this.dir);
		this.logger = Logger.getLogger("FTXL");
		this.header = new TxnHeader(1L, 0, 1L, 1000L, 1);
		this.crTxn = new CreateTxn("/test1", "AAAA".getBytes(), 
				AclParser.parse("world:1:c"), false, 0);
		
		if(forceSync)
			System.setProperty("zookeeper.forceSync", "yes");
		else
			System.setProperty("zookeeper.forceSync", "no");
	}
	
	
	@After
	public void cleanEnv() {
		try {
			FileUtils.deleteDirectory(this.dir);
		} catch (IOException e) {
			this.logger.log(Level.WARNING, "Failed to delete directory\n");
		}
	}
	
	
	@Test
	public void testDiskSize() throws IOException, InterruptedException {
		
		//added to increase coverage
		FileTxnLog.setTxnLogSizeLimit(this.maxSize);
		
		long logSizeBefore = this.log.getCurrentLogSize();
		this.log.append(this.header, this.crTxn);
		this.log.commit();
		long logSizeAfter = this.log.getCurrentLogSize();
		
		//assert that the sizes changed after the append of a transaction in the log
		assertNotEquals(logSizeBefore, logSizeAfter);
		
		this.log.close();
	}
}
