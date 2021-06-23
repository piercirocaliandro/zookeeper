package it.uniroma2.referencecountedaclcache.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.ReferenceCountedACLCache;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestReferenceCountedAclCacheConv {
	private List<ACL> acls;
	private static int size = 3;
	private ReferenceCountedACLCache rcAclCache;
	
	
	@Parameters
	public static Collection<Object> getParams(){
		Integer index;
		
		//create a valid list of ACL
		List<ACL> validList = new ArrayList<>();
		
		for(index = 0; index < size; index++) {
			ACL acl = new ACL();
			Id id = new Id();
			id.setId(index.toString());
			acl.setId(id);
			acl.setPerms(index.intValue());
		}
		
		// create an invalid list of ACL
		List<ACL> invalidList = new ArrayList<>();
		for(index = 0; index < size; index++) {
			ACL acl = new ACL();
			Id id = new Id();
			id.setId(index.toString());
			acl.setId(id);
			acl.setPerms(-1);
		}
		
		List<ACL> emptyList = new ArrayList<>();
		
		return Arrays.asList(new Object[] {
				validList,
				invalidList,
				emptyList,
				null
		});
	}
	
	
	public TestReferenceCountedAclCacheConv(List<ACL> acls) {
		this.configure(acls);
	}
	
	
	private void configure(List<ACL> acls) {
		this.acls = acls;
		this.rcAclCache = new ReferenceCountedACLCache();
	}
	
	
	@Test
	public void testAclsConv() {
		long result = this.rcAclCache.convertAcls(acls);
		assertNotEquals(0, result, 0.0);
		
		assertEquals(this.rcAclCache.convertLong(result), this.acls);
	}
}
