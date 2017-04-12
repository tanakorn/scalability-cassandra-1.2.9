package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uchicago.cs.ucare.cassandra.gms.ReceivedMessageManager.ReceivedMessage;

public class SynchronizationManager {

	private static Logger logger = LoggerFactory.getLogger(SynchronizationManager.class);
	public static final SynchronizationManager instance = new SynchronizationManager();
	
	private String basePath = null;
	private Map<InetAddress, ReentrantLock> lockPerHosts = 
			new ConcurrentHashMap<InetAddress, ReentrantLock>();
	private Map<String, ArrayList<Integer>> processingOrderPerHost = 
			new ConcurrentHashMap<String, ArrayList<Integer>>();
	
	public void initSynchronizationManager(boolean replayEnabled, String basePath, Collection<InetAddress> hosts){
		this.basePath = basePath;
		for(InetAddress host : hosts){
			lockPerHosts.put(host, new ReentrantLock());
			processingOrderPerHost.put(host.toString(), new ArrayList<Integer>());
		}
		if(replayEnabled){
			loadDependencyMapFromFile();
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
	
	public void loadDependencyMapFromFile(){
		String mapFileName = MessageUtil.buildReceivedMessageFilePathForDependencyMap(basePath);
		BufferedReader brdr = null;
		try{
			long sumMessages = 0L;
			File file = new File(mapFileName);
			brdr = new BufferedReader(new FileReader(file));
			String line = null;
			while((line = brdr.readLine()) != null){
				String [] parsed = line.split(MessageUtil.STATE_FIELD_SEP);
				int roundId = Integer.valueOf(parsed[0]);
				String hostId = parsed[1];
				processingOrderPerHost.get(hostId).add(roundId);
				++sumMessages;
			}
			brdr.close();
			logger.info("@Cesar: <" + sumMessages + "> messages loaded  from <" + mapFileName + ">");
		}
		catch(Exception ioe){
			logger.error("@Cesar: Exception while loading <" + mapFileName + ">", ioe);
		}
		finally{
			try{
				if(brdr != null) brdr.close();
			}
			catch(IOException ioe){
				// nothing to do...
			}
		}
	}
	
	public void saveDependencyToFile(int messageRound, String host){
		String mapFileName = MessageUtil.buildReceivedMessageFilePathForDependencyMap(basePath);
		PrintWriter pr = null;
		ObjectOutputStream out = null;
		FileOutputStream fopt = null;
		try{
			pr = new PrintWriter(new FileWriter(new File(mapFileName), true));
			pr.println(messageRound + MessageUtil.STATE_FIELD_SEP + host);
			if(logger.isDebugEnabled()) logger.debug("@Cesar: Message <" + messageRound + ", " + host + "> saved to <" + mapFileName +  ">");
		}
		catch(Exception ioe){
			logger.error("@Cesar: Exception while saving <" + mapFileName + ">", ioe);
		}
		finally{
			if(pr != null) pr.close();
		}

	}
	
}
