package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipRoundManager {

	private static final int INITIAL_CAPACITY = 100;
	private static final Logger logger = LoggerFactory.getLogger(GossipRoundManager.class);
	
	private Map<InetAddress, PriorityBlockingQueue<GossipRound>> sentMessagesPerHost =
				new ConcurrentHashMap<InetAddress, PriorityBlockingQueue<GossipRound>>();
	
	private Map<InetAddress, Integer> roundsPerHost =
			new ConcurrentHashMap<InetAddress, Integer>();
	
	
	public boolean sentMessageQueuesLeft(){
		return queuesLeft(sentMessagesPerHost);
	}
	
	private boolean queuesLeft(Map<InetAddress, PriorityBlockingQueue<GossipRound>> map){
		return map.size() > 0;
	}
	
	public void removeSentMessageQueue(InetAddress host){
		removeMessageQueue(host, sentMessagesPerHost);
	}
	
	private void removeMessageQueue(InetAddress host, 
									Map<InetAddress, PriorityBlockingQueue<GossipRound>> map){
		PriorityBlockingQueue<GossipRound> q = map.get(host);
		if(q != null && q.size() == 0){
			map.remove(host);
		}
	}
	
	public int getNextRoundFor(InetAddress host){
		if(!roundsPerHost.containsKey(host)){
			roundsPerHost.put(host, 1);
			return 0;
		}
		else{
			int current = roundsPerHost.get(host);
			roundsPerHost.put(host, current + 1);
			return current;
		}
	}
	
	public int geSentMessageQueueSize(InetAddress host){
		return getSizeOf(host, sentMessagesPerHost);
	}
	
	private int getSizeOf(InetAddress host, 
						  Map<InetAddress, PriorityBlockingQueue<GossipRound>> map){
		return map.containsKey(host)? map.get(host).size() : 0;
	}
	
	public GossipRound pollNextSentMessage(InetAddress host){
		return pollNext(host, sentMessagesPerHost);
	}
	
	private GossipRound pollNext(InetAddress host, 
								 Map<InetAddress, PriorityBlockingQueue<GossipRound>> map){
		if(!map.containsKey(host)) return null;
		return map.get(host).poll();
	}
	
	public void addSentMessage(InetAddress host, 
							   GossipRound message){
		addToQueue(host, message, sentMessagesPerHost);
	}
	
	private void addToQueue(InetAddress host, 
							GossipRound message, 
							Map<InetAddress, PriorityBlockingQueue<GossipRound>> map){
		if (map.containsKey(host)){
			map.get(host).add(message);
		}
		else{
			PriorityBlockingQueue<GossipRound> pq = 
					new PriorityBlockingQueue<GossipRound>(INITIAL_CAPACITY,
												   			new GossipRound.GossipRoundComparator());
			pq.add(message);
			map.put(host, pq);
			
		}
	}
	
	public void saveRoundToFile(GossipRound round, String basePath, InetAddress id){
		String fileName = MessageUtils.buildSentGossipFilePathForRound(round, basePath, id);
		PrintWriter pr = null;
		try{
			File file = new File(fileName);
			if(!file.getParentFile().exists()) file.getParentFile().mkdirs(); 
			pr = new PrintWriter(file);
			String serializedRound = GossipRound.messageToString(round);
			pr.println(serializedRound);
			logger.info("@Cesar: Round <" + round.getGossipRound() + "> saved to <" + fileName + ">");
		}
		catch(Exception ioe){
			logger.error("@Cesar: Exception while saving <" + fileName + ">", ioe);
		}
		finally{
			if(pr != null) pr.close();
		}
	}
	
	private void loadFromFile(String basePath, InetAddress id){
		String directoryName = MessageUtils.buildSentGossipFilePath(basePath, id);
		BufferedReader brdr = null;
		try{
			File dir = new File(directoryName);
			File[] allSentMessages = dir.listFiles();
			for(File message : allSentMessages){
				try{
					brdr = new BufferedReader(new FileReader(message));
					String serialized = brdr.readLine();
					GossipRound reconstructed = GossipRound.messageFromString(serialized);
					addSentMessage(id, reconstructed);
				}
				catch(Exception e){
					logger.error("@Cesar: Skipped a message since cannot load", e);
				}
			}
			logger.info("@Cesar: <" + geSentMessageQueueSize(id) + "> gossip rounds loaded " + 
						 "for host <" + id + "> from <" + directoryName + ">");
		}
		catch(Exception ioe){
			logger.error("@Cesar: Exception while loading <" + directoryName + ">", ioe);
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
	
	public void loadSentGossipRounds(String basePath, List<InetAddress> addressList){
		sentMessagesPerHost.clear();
		for(InetAddress address : addressList) loadFromFile(basePath, address);
	}
	
	
	
}
