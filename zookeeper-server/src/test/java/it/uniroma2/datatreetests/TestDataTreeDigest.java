package it.uniroma2.datatreetests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;


import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.DigestWatcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import it.uniroma2.datatree.utils.DataTreeTestCommon;


@RunWith(value = Parameterized.class)
public class TestDataTreeDigest extends DataTreeTestCommon{
	private TxnHeader header;
	private Stat stat;
	private TxnDigest digest;
	
	private File ioFile;
	private long lastProcZxid;
	
	private boolean isEnabledDigest;
	
	private DataTree dt;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{new TxnHeader(1L, 1, 1L, 1000L, 0), 
				new Stat(1L, 1L, 1000L, 1000L, 1, 1, 1, 1L, 3, 0, 2L), 
				new TxnDigest(1, 1L), new File("ser_des_digest.txt"), -1L, true},
			
			{new TxnHeader(1L, 1, 1L, 1000L, 0), 
				null, 
				new TxnDigest(1, 0L), new File("ser_des_digest.txt"), -1L, true},
			{new TxnHeader(1L, 1, 1L, 1000L, 0), 
				null, 
				new TxnDigest(0, 2L), new File("ser_des_digest.txt"), -1L, false},
			{new TxnHeader(1L, 1, 1L, 1000L, 0), 
				new Stat(1L, 1L, 1000L, 1000L, 1, 1, 1, 1L, 3, 0, 2L), 
				new TxnDigest(0, -1L), new File("ser_des_digest.txt"), Long.MIN_VALUE, true},
			{new TxnHeader(1L, 1, 1L, 1000L, 0), 
				new Stat(1L, 1L, 1000L, 1000L, 1, 1, 1, 1L, 3, 0, 2L), 
				new TxnDigest(-1, 1L), new File("ser_des_digest.txt"), Long.MAX_VALUE, true},
			{new TxnHeader(1L, 1, 1L, 1000L, 0), 
				new Stat(1L, 1L, 1000L, 1000L, 1, 1, 1, 1L, 3, 0, 2L), 
				new TxnDigest(1, Long.MAX_VALUE), new File("ser_des_digest.txt"), 1L, true},
			{new TxnHeader(1L, 1, 1L, 1000L, 0), 
				new Stat(1L, 1L, 1000L, 1000L, 1, 1, 1, 1L, 3, 0, 2L), 
				new TxnDigest(0, 1L), new File("ser_des_digest.txt"), 0L, true}
		});
	}
	
	
	public TestDataTreeDigest(TxnHeader header, Stat stat, TxnDigest digest, File ioFile, 
			long lastProcZxid, boolean isEnabled) throws IOException {
		this.configure(header, stat, digest, ioFile, lastProcZxid, isEnabled);
	}
	
	public void configure(TxnHeader header, Stat stat, TxnDigest digest, File ioFile, 
			long lastProcZxid, boolean isEnabled) throws IOException {
		this.header = header;
		this.stat = stat;
		this.digest = digest;
		this.ioFile = ioFile;
		if(ioFile != null)
			this.ioFile.createNewFile();
		
		this.lastProcZxid = lastProcZxid;
		this.isEnabledDigest = isEnabled;
		ZooKeeperServer.setDigestEnabled(this.isEnabledDigest);
		
		this.dt = new DataTree();
		
		this.createNodes(this.getNodes(), this.dt);
	}
	
	
	@After
	public void cleanEnv() {
		if(this.ioFile != null)
			this.ioFile.delete();
	}
	
	
	@Test
	public void testCompareDigest() {
		assertTrue(this.dt.compareDigest(this.header, this.stat, this.digest));
	}
	
	
	@Test
	public void testSerDesDigest() throws IOException {
		
		BinaryInputArchive bia = new BinaryInputArchive(new DataInputStream(
				new FileInputStream(this.ioFile)));
		BinaryOutputArchive boa = new BinaryOutputArchive(new DataOutputStream(
				new FileOutputStream(this.ioFile)));
		
		if(this.isEnabledDigest) {
			assertTrue(this.dt.serializeZxidDigest(boa));
		
			assertTrue(this.dt.deserializeZxidDigest(bia, this.lastProcZxid));
		}
		else {
			assertFalse(this.dt.serializeZxidDigest(boa));
			
			assertFalse(this.dt.deserializeZxidDigest(bia, this.lastProcZxid));
		}
	}
	
	
	@Test
	public void testReportDigestMism() {
		
		// mock the DigestWatcher interface
		DigestWatcher mockedWatcher = mock(DigestWatcher.class);
		this.dt.addDigestWatcher(mockedWatcher);
		this.dt.reportDigestMismatch(this.lastProcZxid);
		
		verify(mockedWatcher).process(this.lastProcZxid);
	}
}
