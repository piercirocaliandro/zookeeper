package it.uniroma2.datatreetests;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.server.DataTree;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/* Test methods that copy Stats */

@RunWith(value = Parameterized.class)
public class TestDataTreeCopyStats {
	private Stat statFrom;
	private Stat statTo;
	private StatPersisted statPFrom;
	private StatPersisted statPTo;
	
	
	@Parameters
	public static Collection<Object[]> getParams(){
		return Arrays.asList(new Object[][] {
			{new Stat(1L, 1L, 1000L, 1000L, 1, 1, 1, 1L, 3, 0, 2L), new Stat(), 
				new StatPersisted(1L, 1L, 1000L, 1000L, 1, 1, 1, 1L, 1L), new StatPersisted()},
		});
	}
	
	
	public TestDataTreeCopyStats(Stat statFrom, Stat statTo, StatPersisted statPFrom, 
			StatPersisted statPTo) {
		this.configure(statFrom, statTo, statPFrom, statPTo);
	}
	
	
	private void configure(Stat statFrom, Stat statTo, StatPersisted statPFrom, 
			StatPersisted statPTo) {
		this.statFrom = statFrom;
		this.statTo = statTo;
		this.statPFrom = statPFrom;
		this.statPTo = statPTo;
	}
	
	
	@Test
	public void testCopyStat() {
		DataTree.copyStat(this.statFrom, this.statTo);
		assertEquals(this.statFrom, this.statTo);
	}
	
	
	@Test
	public void testCopyStatP() {
		DataTree.copyStatPersisted(this.statPFrom, this.statPTo);
		assertEquals(this.statPFrom, this.statPTo);
	}
}
