package it.uniroma2.filetxnlogtests;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.zookeeper.server.persistence.FilePadding;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(value = Parameterized.class)
public class TestFileTxnLogSetPreallocSize {
	private long size;

	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{1L},
			{-1L},
			{Long.MAX_VALUE},
			{1L},
		});
	}
	
	
	public TestFileTxnLogSetPreallocSize(long size) {
		this.configure(size);
	}
	
	
	private void configure(long size) {
		this.size = size;
	}
	
	
	@Test
	public void testSetPreallocSize() {
		FileTxnLog.setPreallocSize(size);
		assertEquals(size, FilePadding.getPreAllocSize());
	}
}
