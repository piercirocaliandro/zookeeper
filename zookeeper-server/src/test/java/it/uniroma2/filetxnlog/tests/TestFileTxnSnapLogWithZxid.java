package it.uniroma2.filetxnlog.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.FileTxnLog.FileTxnIterator;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.TxnLog;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import it.uniroma2.txnsnaplog.util.RequestFactoryUtil;

@RunWith(value = Parameterized.class)
public class TestFileTxnSnapLogWithZxid {
	private long zxid;
	private boolean fastForward;
	
	private File logDir;
	private FileTxnLog txnLog;
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{1L,true},
			/*{2L,true},
			{1L,true},
			{-1L,true},
			{Long.MAX_VALUE,false},
			{null,false}*/
		});
	}
	
	
	public TestFileTxnSnapLogWithZxid(long zxid, boolean fastForward) throws IOException {
		this.configure(zxid);
	}
	
	
	private void configure(Long zxid) throws IOException {
		BasicConfigurator.configure();
		this.zxid = zxid;
		this.logDir = new File("logdir");
		this.logDir.mkdir();

		this.txnLog = new FileTxnLog(this.logDir);
		
		Map<TxnHeader, Id> elems = this.getLogEntries(2);
		for(TxnHeader header : elems.keySet()) {
			System.out.println(this.txnLog.append(header, elems.get(header)));
		}
	}
	
	
	private Map<TxnHeader, Id> getLogEntries(long count){
		Map<TxnHeader, Id> logMap = new HashMap<>();
		long zxid = 2;
		for(long i = 0; i < count; i++) {
			
			TxnHeader header = new TxnHeader();
			header.setZxid(zxid);
			
			header.setClientId(i);
			header.setCxid(1);
			header.setTime(1000L);
			header.setType(OpCode.setWatches);
			/*Stat stat = new Stat();
			stat.setCzxid(zxid);
			stat.setMzxid(zxid);
			stat.setPzxid(zxid);
			stat.setAversion(1);
			stat.setCtime(1000L);
			stat.setDataLength(10);
			stat.setEphemeralOwner(zxid);
			stat.setMtime(1000L);
			stat.setNumChildren(1);
			stat.setVersion(1);*/
			Id id = new Id();
			id.setId(Long.valueOf(zxid).toString());
			id.setScheme("world");
			
			
			logMap.put(header, id);
			zxid++;
		}
		
		return logMap;
	}
	
	
	@Test
	public void testWithZxid() throws IOException {
		FileTxnIterator iterator = (FileTxnIterator) this.txnLog.read(this.zxid);
		assertFalse(iterator.next());
		iterator.close();
		this.txnLog.close();
	}
}
