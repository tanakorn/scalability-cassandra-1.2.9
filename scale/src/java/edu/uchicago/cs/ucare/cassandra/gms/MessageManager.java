package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageManager{

	private static final int INITIAL_CAPACITY = 100;
	private static final Logger logger = LoggerFactory.getLogger(MessageManager.class);
	
	private Map<InetAddress, PriorityBlockingQueue> sentMessagesPerHost =
				new ConcurrentHashMap<InetAddress, PriorityBlockingQueue>();
	private Map<InetAddress, PriorityBlockingQueue> receivedMessagesPerHost =
			new ConcurrentHashMap<InetAddress, PriorityBlockingQueue>();
	
	private Map<InetAddress, Integer> gossipRoundsPerHost =
			new ConcurrentHashMap<InetAddress, Integer>();
	
	private Map<InetAddress, Integer> receivedPerHost =
			new ConcurrentHashMap<InetAddress, Integer>();
	
	public boolean sentMessageQueuesLeft(){
		return queuesLeft(sentMessagesPerHost);
	}
	
	public boolean receivedMessageQueuesLeft(){
		return queuesLeft(receivedMessagesPerHost);
	}
	
	private boolean queuesLeft(Map<InetAddress, PriorityBlockingQueue> map){
		return map.size() > 0;
	}
	
	public void removeReceivedMessageQueue(InetAddress host){
		removeMessageQueue(host, receivedMessagesPerHost);
	}
	
	public void removeSentMessageQueue(InetAddress host){
		removeMessageQueue(host, sentMessagesPerHost);
	}
	
	private void removeMessageQueue(InetAddress host, 
									Map<InetAddress, PriorityBlockingQueue> map){
		PriorityBlockingQueue q = map.get(host);
		if(q != null && q.size() == 0){
			map.remove(host);
		}
	}
	
	public int getNextRoundFor(InetAddress host){
		if(!gossipRoundsPerHost.containsKey(host)){
			gossipRoundsPerHost.put(host, 1);
			return 0;
		}
		else{
			int current = gossipRoundsPerHost.get(host);
			gossipRoundsPerHost.put(host, current + 1);
			return current;
		}
	}
	
	public int getNextReceivedFor(InetAddress host){
		if(!receivedPerHost.containsKey(host)){
			receivedPerHost.put(host, 1);
			return 0;
		}
		else{
			int current = receivedPerHost.get(host);
			receivedPerHost.put(host, current + 1);
			return current;
		}
	}
	
	public int getSentMessageQueueSize(InetAddress host){
		return getSizeOf(host, sentMessagesPerHost);
	}
	
	public int getReceivedMessageQueueSize(InetAddress host){
		return getSizeOf(host, receivedMessagesPerHost);
	}
	
	private int getSizeOf(InetAddress host, 
						  Map<InetAddress, PriorityBlockingQueue> map){
		return map.containsKey(host)? map.get(host).size() : 0;
	}
	
	public GossipRound pollNextSentMessage(InetAddress host){
		return (GossipRound)pollNext(host, sentMessagesPerHost);
	}
	
	public ReceivedMessage pollNextReceivedMessage(InetAddress host){
		return (ReceivedMessage)pollNext(host, receivedMessagesPerHost);
	}
	
	private Object pollNext(InetAddress host, 
							Map<InetAddress, PriorityBlockingQueue> map){
		if(!map.containsKey(host)) return null;
		return map.get(host).poll();
	}
	
	public void addSentMessage(InetAddress host, 
							   GossipRound message){
		addToQueue(host, message, new GossipRound.GossipRoundComparator(), sentMessagesPerHost);
	}
	
	public void addReceivedMessage(InetAddress host, 
								   ReceivedMessage message){
		addToQueue(host, message, new ReceivedMessage.ReceivedMessageComparator(), receivedMessagesPerHost);
	}
	
	private void addToQueue(InetAddress host, 
							Object message, 
							Comparator comp,
							Map<InetAddress, PriorityBlockingQueue> map){
		if (map.containsKey(host)){
			map.get(host).add(message);
		}
		else{
			PriorityBlockingQueue pq = 
					new PriorityBlockingQueue(INITIAL_CAPACITY, comp);
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
			logger.debug("@Cesar: Round <" + round.getGossipRound() + "> saved to <" + fileName + ">");
		}
		catch(Exception ioe){
			logger.error("@Cesar: Exception while saving <" + fileName + ">", ioe);
		}
		finally{
			if(pr != null) pr.close();
		}
	}
	
	public void saveMessageToFile(ReceivedMessage message, String basePath, InetAddress id){
		String fileName = MessageUtils.buildReceivedMessageFilePathForRound(message, basePath, id);
		PrintWriter pr = null;
		try{
			File file = new File(fileName);
			if(!file.getParentFile().exists()) file.getParentFile().mkdirs(); 
			pr = new PrintWriter(file);
			String serializedRound = ReceivedMessage.messageToString(message);
			pr.println(serializedRound);
			logger.debug("@Cesar: Message <" + message.getMessageRound() + "> saved to <" + fileName + ">");
		}
		catch(Exception ioe){
			logger.error("@Cesar: Exception while saving <" + fileName + ">", ioe);
		}
		finally{
			if(pr != null) pr.close();
		}
	}
	
	private void loadRoundsFromFile(String basePath, InetAddress id){
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
					brdr.close();
				}
				catch(Exception e){
					// logger.error("@Cesar: Skipped a message since cannot load", e);
				}
			}
			logger.info("@Cesar: <" + getSentMessageQueueSize(id) + "> gossip rounds loaded " + 
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
	
	private void loadMessagesFromFile(String basePath, InetAddress id){
		String directoryName = MessageUtils.buildReceivedMessageFilePath(basePath, id);
		BufferedReader brdr = null;
		try{
			File dir = new File(directoryName);
			File[] allMessages = dir.listFiles();
			for(File message : allMessages){
				try{
					brdr = new BufferedReader(new FileReader(message));
					String serialized = brdr.readLine();
					ReceivedMessage reconstructed = ReceivedMessage.messageFromString(serialized);
					addReceivedMessage(id, reconstructed);
					brdr.close();
				}
				catch(Exception e){
					// logger.error("@Cesar: Skipped a message since cannot load", e);
				}
			}
			logger.info("@Cesar: <" + getReceivedMessageQueueSize(id) + "> messages loaded " + 
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
		for(InetAddress address : addressList) loadRoundsFromFile(basePath, address);
	}
	
	public void loadReceivedMessages(String basePath, List<InetAddress> addressList){
		receivedMessagesPerHost.clear();
		for(InetAddress address : addressList) loadMessagesFromFile(basePath, address);
	}
	
}
