package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.CloseSessionTxn;
import org.apache.zookeeper.txn.CreateContainerTxn;
import org.apache.zookeeper.txn.CreateTTLTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestDataTreeTxn {
	private TxnHeader header;
	private Record record;
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
			/*{null, new CreateTxn("/", "aaaa".getBytes(), AclParser.parse("world:1:c"), true, 1), 
						new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.create), null, 
							new TxnDigest(1, 1L)},*/
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.create), 
                new CreateTxn("/", "aaaa".getBytes(), AclParser.parse("world:1:c"), true, 1), 
                null},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.create), 
			    new CreateTxn("/asf", "aaaa".getBytes(), AclParser.parse("afr:1:v"), true, -1), 
			    new TxnDigest(1, 1L)},
			
			// added to increase coverage
			
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.create2), 
	                new CreateTxn("/", "aaaa".getBytes(), AclParser.parse("world:1:c"), true, 1), 
	                new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.createTTL), 
	                new CreateTTLTxn("/", "abc".getBytes(), AclParser.parse("world:1:c"), 1, 1000L), 
		            new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.createContainer), 
		            new CreateContainerTxn("/", "abc".getBytes(), AclParser.parse("world:1:c"), 1), 
			        new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.delete), 
				    new DeleteTxn("/"), 
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.deleteContainer), 
			        new DeleteTxn("/"), 
				    new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.reconfig), 
				    new SetDataTxn("/", "aaaa".getBytes(), 1), 
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.setData), 
					new SetDataTxn("/", "aaaa".getBytes(), 1), 
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.setACL), 
					new SetACLTxn("/", AclParser.parse("world:1:c"), 1), 
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.closeSession), 
					new CloseSessionTxn(Arrays.asList("/")), new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.closeSession), 
					null, new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.error), 
					new ErrorTxn(1), new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.check), 
					new CheckVersionTxn("/", 1), 
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.multi), 
				    new MultiTxn(Arrays.asList(new Txn(OpCode.createTTL, "aaaa".getBytes()))),
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.multi), 
					new MultiTxn(Arrays.asList(new Txn(OpCode.createContainer, "aaaa".getBytes()))),
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.multi), 
					new MultiTxn(Arrays.asList(new Txn(OpCode.delete, "aaaa".getBytes()))),
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.multi), 
					new MultiTxn(Arrays.asList(new Txn(OpCode.deleteContainer, "aaaa".getBytes()))),
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.multi), 
					new MultiTxn(Arrays.asList(new Txn(OpCode.setData, "aaaa".getBytes()))),
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.multi), 
					new MultiTxn(Arrays.asList(new Txn(OpCode.error, "aaaa".getBytes()))),
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.multi), 
					new MultiTxn(Arrays.asList(new Txn(OpCode.check, "aaaa".getBytes()))),
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.multi), 
					new MultiTxn(Arrays.asList(new Txn(OpCode.create, "aaaa".getBytes()))),
					new TxnDigest(1, 1L)},
			{new TxnHeader(1L, 1, 1L, 1000L, OpCode.multi), 
					new MultiTxn(Arrays.asList(new Txn(OpCode.sasl, "aaaa".getBytes()))),
					new TxnDigest(1, 1L)},
			
			{new TxnHeader(0L, 1, -1L, 1000L, OpCode.create), 
					new CreateTxn("/asf", "aaaa".getBytes(), AclParser.parse("world:1:c"), false, 1), 
					new TxnDigest(1, 1L)},
			{new TxnHeader(0L, 1, -1L, 1000L, OpCode.create2), 
					new CreateTxn("/asf", "aaaa".getBytes(), AclParser.parse("world:1:c"), false, 1), 
					new TxnDigest(1, 1L)},
			{new TxnHeader(0L, 1, -1L, 1000L, OpCode.create), 
					new CreateTxn("/asf", "aaaa".getBytes(), AclParser.parse("world:1:c"), false, 1), 
					new TxnDigest(1, 1L)},
			/*{new TxnHeader(1L, 1, 1L, 1000L, OpCode.multi), 
					null,  new TxnDigest(1, 1L)},*/
		});
		
	}
	
	
	public TestDataTreeTxn(TxnHeader header, Record r, TxnDigest digest) {
		this.configure(header, r, digest);
	}
	
	
	private void configure(TxnHeader header, Record r, TxnDigest digest) {
		this.header = header;
		this.record = r;
		this.digest = digest;
		
		this.dt = new DataTree();
	}
	
	
	@Test
	public void testTxnProcess() {
		ProcessTxnResult res = this.dt.processTxn(this.header, this.record);
		assertEquals(res.zxid, this.header.getZxid());
	}
	
	
	@Test
	public void testTxnProcessWithDigest() {
		ProcessTxnResult res = this.dt.processTxn(this.header, this.record, this.digest);
		assertEquals(res.cxid, this.header.getCxid());
	}
	
	
	@Test
	public void testTxnProcessWithSub() {
		ProcessTxnResult res = this.dt.processTxn(this.header, this.record, this.isSubTxn);
		assertEquals(res.clientId, this.header.getClientId());
	}
}
