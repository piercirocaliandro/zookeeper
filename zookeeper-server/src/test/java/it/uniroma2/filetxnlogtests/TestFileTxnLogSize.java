package it.uniroma2.filetxnlogtests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
public class TestFileTxnLogSize {
	private long logSize;
	
	private FileTxnLog log;
	private File dir;
	private Logger logger;
	
	
	@Parameters
	public static Collection<Long> getParams(){
		return Arrays.asList(new Long[] {
				123456789L,
				-1L,
				0L,
				Long.MAX_VALUE,
		});
	}
	
	
	public TestFileTxnLogSize(long logSize) {
		this.conifgure(logSize);
	}
	
	
	private void conifgure(long logSize) {
		this.logSize = logSize;
		
		this.dir = new File("logdir");
		this.dir.mkdir();
		this.log = new FileTxnLog(this.dir);
		
		this.logger = Logger.getLogger("TXNL");
	}
	
	
	@After
	public void cleanEnv() {
		try {
			FileUtils.deleteDirectory(this.dir);
		} catch (IOException e) {
			this.logger.log(Level.SEVERE, "Failed to close directory\n");
		}
	}
	
	
	@Test
	public void testTotSize() throws IOException {
		this.log.setTotalLogSize(this.logSize);
		assertEquals(this.log.getTotalLogSize(), this.logSize);
		
		if(this.logSize > 0)
			assertFalse(this.log.getTotalLogSize() < 0);
		else
			assertTrue(this.log.getTotalLogSize() <= 0);
		
		this.log.close();
	}
}
