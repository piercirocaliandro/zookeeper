package it.uniroma2.filetxnlog.tests;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestFileTnxLogCreation {
	private File logDir;
	private Logger logger;
	
	
	@Parameters
	public static Collection<String> getParams(){
		return Arrays.asList(new String[] {
			"logdir",
			"logdir",
			"logdir",
			"logdir",
			null,
		});
	}
	
	
	public TestFileTnxLogCreation(String dataDir) 
			throws IOException {
		this.configure(dataDir);
	}
	
	
	private void configure(String dataDir) throws IOException {
		if(dataDir != null) {
			this.logDir = new File(dataDir);
			this.logDir.mkdir();
		}
		else {
			this.logDir = null;
		}
		this.logger = Logger.getLogger("TXN"); 
	}
	
	
	@After
	public void cleanEnv() {
		try {
			if(this.logDir != null)
				FileUtils.deleteDirectory(this.logDir);
		} catch (IOException e) {
			this.logger.log(Level.WARNING, "Failed to delete directory\n");
		}
	}
	
	
	@Test
	public void testAppend() throws IOException {
		FileTxnLog txnLog = new FileTxnLog(this.logDir);
		assertNotNull(txnLog);
		txnLog.close();
		
		//Read the data after the commit
		/*FileTxnLog.FileTxnIterator fileTxnIterator = new FileTxnLog.FileTxnIterator(this.logDir, 1L);
		CreateTxn fetchTxn = (CreateTxn) fileTxnIterator.getTxn();
		CreateTxn record = (CreateTxn)this.record;
		
		System.out.println(fetchTxn.getPath()+" "+record.getPath());
	    assertEquals(fetchTxn.getPath(), record.getPath());
	    
	    fileTxnIterator.close();*/
	}
}
