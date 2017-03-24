package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipRoundManager {

	private static final int INITIAL_CAPACITY = 100;
	private static final Logger logger = LoggerFactory.getLogger(GossipRoundManager.class);
	
	private Map<InetAddress, PriorityQueue<GossipRound>> sentMessagesPerHost =
				new ConcurrentHashMap<InetAddress, PriorityQueue<GossipRound>>();
	
	private Map<InetAddress, Integer> roundsPerHost =
			new ConcurrentHashMap<InetAddress, Integer>();
	
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
						  Map<InetAddress, PriorityQueue<GossipRound>> map){
		return map.containsKey(host)? map.get(host).size() : 0;
	}
	
	public GossipRound pollNextSentMessage(InetAddress host){
		return pollNext(host, sentMessagesPerHost);
	}
	
	private GossipRound pollNext(InetAddress host, 
								 Map<InetAddress, PriorityQueue<GossipRound>> map){
		if(!map.containsKey(host)) return null;
		return map.get(host).poll();
	}
	
	public void addSentMessage(InetAddress host, 
							   GossipRound message){
		addToQueue(host, message, sentMessagesPerHost);
	}
	
	private void addToQueue(InetAddress host, 
							GossipRound message, 
							Map<InetAddress, PriorityQueue<GossipRound>> map){
		if (map.containsKey(host)){
			map.get(host).add(message);
		}
		else{
			PriorityQueue<GossipRound> pq = 
					new PriorityQueue<GossipRound>(INITIAL_CAPACITY,
												   new GossipRound.GossipRoundComparator());
			pq.add(message);
			map.put(host, pq);
			
		}
	}
	
	public void saveRoundToFile(GossipRound round, String basePath, InetAddress id){
		String fileName = MessageUtils.buildSentGossipFilePathForRound(round, basePath, id);
		PrintWriter pr = null;
		try{
			pr = new PrintWriter(new File(fileName));
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
				brdr = new BufferedReader(new FileReader(message));
				String serialized = brdr.readLine();
				GossipRound reconstructed = GossipRound.messageFromString(serialized);
				addSentMessage(id, reconstructed);
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
