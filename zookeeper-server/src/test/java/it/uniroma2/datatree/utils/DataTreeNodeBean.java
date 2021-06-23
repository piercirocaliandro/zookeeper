package it.uniroma2.datatree.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.data.ACL;

/* Useful data bean to test DataTree class*/
public class DataTreeNodeBean {
	private String path;
	private byte[] data;
	private List<ACL> acl;
	private long ephemeralOwner; // -1 if the node is not ephemeral
    private int parentCVersion;
    private long zxid;
    private long time;
    
    public DataTreeNodeBean(String path, byte[] data, String acls, 
    		long ephemeralOwner, int parentCVersion, long zxid, long time) {
    	this.path = path;
    	this.data = data;
    	this.ephemeralOwner = ephemeralOwner;
    	this.parentCVersion = parentCVersion;
    	this.zxid = zxid;
    	this.time = time;
    	
    	if(acls.isEmpty())
    		this.acl = new ArrayList<>();
    	else if(acls.contentEquals("null"))
    		this.acl = null;
    	else
    		this.acl = AclParser.parse(acls);
    }
    
    
    public String getPath() {
    	return this.path;
    }
    
    public byte[] getData() {
    	return this.data;
    }
    
    public List<ACL> getAcls(){
    	return this.acl;
    }
    
    public long getEphemeralOwner() {
    	return this.ephemeralOwner;
    }
    
    public int getParentCVers() {
    	return this.parentCVersion;
    }
    
    public long getZxid() {
    	return this.zxid;
    }
    
    public long getTime() {
    	return this.time;
    }
}
