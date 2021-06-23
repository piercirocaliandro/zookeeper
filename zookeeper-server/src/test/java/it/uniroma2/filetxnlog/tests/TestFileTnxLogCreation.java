package it.uniroma2.filetxnlog.tests;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestFileTnxLogCreation {
	private File logDir;
	private TxnHeader header;
	private Stat record;
	private FileTxnLog txnLog;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{"logdir", new TxnHeader(), new Stat()},
			{"", new TxnHeader(), new Stat()},
			{"logdir", null, new Stat()},
			{"logdir", new TxnHeader(), null},
			{null, new TxnHeader(), new Stat()},
			{"/logdir", new TxnHeader(), new Stat()},
		});
	}
	
	
	public TestFileTnxLogCreation(String dataDir, TxnHeader header, Stat stat) 
			throws IOException {
		this.configure(dataDir, header, stat);
	}
	
	
	private void configure(String dataDir, TxnHeader header, Stat stat) throws IOException {
		if(dataDir != null) {
			this.logDir = new File(dataDir);
			this.logDir.mkdir();
		}
		else {
			this.logDir = null;
		}
		
		this.header = header;
		this.record = stat;
		this.txnLog = new FileTxnLog(this.logDir);
	}
	
	
	@Test
	public void testAppend() throws IOException {
		assertTrue(this.txnLog.append(this.header, this.record));
		this.txnLog.close();
	}
}
