package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class SynchronizationManager {

	public static final SynchronizationManager instance = new SynchronizationManager();
	
	private Map<InetAddress, ReentrantLock> lockPerHosts = 
			new ConcurrentHashMap<InetAddress, ReentrantLock>();
	
	public void initSynchronizationManager(Collection<InetAddress> hosts){
		for(InetAddress host : hosts){
			lockPerHosts.put(host, new ReentrantLock());
		}
	}
	
	public void lockHost(InetAddress host){
		ReentrantLock hostLock = lockPerHosts.get(host);
		if(hostLock != null) hostLock.lock();
	}
	
	public void unlockHost(InetAddress host){
		ReentrantLock hostLock = lockPerHosts.get(host);
		if(hostLock != null) hostLock.unlock();
	}
	
	
}
