package it.uniroma2;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.util.AuthUtil;
import org.junit.Test;

public class TestAuthUtil {
	
	@Test
	public void testGetUsersBranch1() {
		assertNull(AuthUtil.getUsers(null));
	}
	
	@Test
	public void testGetUsersBranch2() {
		List<Id> users = new ArrayList<>();
		for(int i = 0; i < 3; i++) {
			Id id = new Id();
			id.setId("id"+i);
			users.add(id);
		}
		assertNull(AuthUtil.getUsers(users));
	}
}
