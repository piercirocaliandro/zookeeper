package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestDataTreeTxn {
	private TxnHeader header;
	private CreateTxn crTxn;
	private TxnDigest digest;
	private boolean isSubTxn;
	
	private DataTree dt;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.create), 
				new CreateTxn("/", "aaaa".getBytes(), AclParser.parse("world:1:c"), true, 1), 
				new TxnDigest(1, 1L)},
			{new TxnHeader(-1L, 1, -3L, 1000L, OpCode.create), 
					new CreateTxn("/", "aaaa".getBytes(), AclParser.parse("world:1:c"), true, 1), 
					new TxnDigest(1, 1L)},
			{null, new CreateTxn("/", "aaaa".getBytes(), AclParser.parse("world:1:c"), true, 1), 
						new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.create), null, 
							new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.create), 
                new CreateTxn("/", "aaaa".getBytes(), AclParser.parse("world:1:c"), true, 1), 
                null},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.create), 
			    new CreateTxn("/asf", "aaaa".getBytes(), AclParser.parse("afr:1:v"), true, -1), 
			    new TxnDigest(1, 1L)},
							
		});
		
	}
	
	
	public TestDataTreeTxn(TxnHeader header, CreateTxn txn, TxnDigest digest) {
		this.configure(header, txn, digest);
	}
	
	
	private void configure(TxnHeader header, CreateTxn txn, TxnDigest digest) {
		this.header = header;
		this.crTxn = txn;
		this.digest = digest;
		
		this.dt = new DataTree();
	}
	
	
	@Test
	public void testTxnProcess() {
		ProcessTxnResult res = this.dt.processTxn(this.header, this.crTxn);
		assertEquals(res.zxid, this.header.getZxid());
	}
	
	
	@Test
	public void testTxnProcessWithDigest() {
		ProcessTxnResult res = this.dt.processTxn(this.header, this.crTxn, this.digest);
		assertEquals(res.cxid, this.header.getCxid());
	}
	
	
	@Test
	public void testTxnProcessWithSub() {
		ProcessTxnResult res = this.dt.processTxn(this.header, this.crTxn, this.isSubTxn);
		assertEquals(res.clientId, this.header.getClientId());
	}
}
