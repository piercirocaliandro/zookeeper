package it.uniroma2.txnsnaplogtests;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.FileTxnLog.FileTxnIterator;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.TxnLog;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import it.uniroma2.txnsnaplog.util.RequestFactoryUtil;

@RunWith(value = Parameterized.class)
public class TestFileTxnSnapLogWithZxid {
	private Long zxid;
	private FileTxnSnapLog txnLog;
	private File dataDir;
	private File snapDir;
	
	@Parameters
	public static Collection<Long> getParams(){
		return Arrays.asList(new Long[] {
			1L,
			/*-1L,
			Long.MAX_VALUE,
			null,*/
		});
	}
	
	
	public TestFileTxnSnapLogWithZxid(Long zxid) throws IOException {
		this.configure(zxid);
	}
	
	
	private void configure(Long zxid) throws IOException {
		BasicConfigurator.configure();
		this.zxid = zxid;
		this.dataDir = new File("logdir");
		this.snapDir = new File("snapdir");
		dataDir.mkdir();
		snapDir.mkdir();

		this.txnLog = new FileTxnSnapLog(this.dataDir, this.snapDir);
		
		/*for(Request elem : RequestFactoryUtil.getRequestListForTesting(3)) {
			this.txnLog.append(elem);
		}*/
		TxnHeader header = new TxnHeader();
		header.setZxid(1L);
		Id id = new Id();
		id.setId("1");
		id.setScheme("world");
		
		Request r = new Request(1L, 1, 2, header, id, 1L);
		this.txnLog.append(r);
	}
	
	
	@Test
	public void testWithZxid() throws IOException {
		//File[] snaps = this.txnLog.getSnapshotLogs(this.zxid.longValue());
		
		FileTxnIterator iterator = (FileTxnIterator) this.txnLog.readTxnLog(this.zxid.longValue());
		System.out.println(iterator.next());
	}
}
