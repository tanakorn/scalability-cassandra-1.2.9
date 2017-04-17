package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageManager{

	private static final int INITIAL_CAPACITY = 100;
	private static final Logger logger = LoggerFactory.getLogger(MessageManager.class);
	
	private Map<InetAddress, ArrayList> sentMessagesPerHost =
				new HashMap<InetAddress, ArrayList>();
	private Map<InetAddress, ArrayList> receivedMessagesPerHost =
				new HashMap<InetAddress, ArrayList>();
	
	private Map<InetAddress, Integer> gossipRoundsPerHost =
			new ConcurrentHashMap<InetAddress, Integer>();
	
	private Map<InetAddress, Integer> receivedPerHost =
			new ConcurrentHashMap<InetAddress, Integer>();
	
	private Map<InetAddress, ReentrantLock> locksPerHost =
			new ConcurrentHashMap<InetAddress, ReentrantLock>();
	
	public boolean sentMessageQueuesLeft(){
		return queuesLeft(sentMessagesPerHost);
	}
	
	public boolean receivedMessageQueuesLeft(){
		return queuesLeft(receivedMessagesPerHost);
	}
	
	private boolean queuesLeft(Map<InetAddress, ArrayList> map){
		return map.size() > 0;
	}
	
	public void removeReceivedMessageQueue(InetAddress host){
		removeMessageQueue(host, receivedMessagesPerHost);
	}
	
	public void removeSentMessageQueue(InetAddress host){
		removeMessageQueue(host, sentMessagesPerHost);
	}
	
	private void removeMessageQueue(InetAddress host, 
									Map<InetAddress, ArrayList> map){
		ArrayList q = map.get(host);
		if(q != null && q.size() == 0){
			synchronized(map) {
				map.remove(host);
			}
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
						  Map<InetAddress, ArrayList> map){
		return map.containsKey(host)? map.get(host).size() : 0;
	}
	
	public GossipRound pollNextSentMessage(InetAddress host, String basePath){
		// in here we reconstruct
		Integer nextMessageId = pollNext(host, sentMessagesPerHost);
		if(nextMessageId == null) return null;
		String messageFileName = 
				MessageUtils.buildSentGossipFilePathForRound(new GossipRound(nextMessageId), basePath, host);
		// now load
		FileInputStream strm = null;
		ObjectInputStream in = null;
		try{
			strm = new FileInputStream(messageFileName);
			in = new ObjectInputStream(strm);
			GossipRound reconstructed = (GossipRound)in.readObject();
			return reconstructed;
		}
		catch(Exception e){
			logger.error("@Cesar: Skipped a message < " + nextMessageId + ">since cannot load", e);
			return null;
		}
		finally{
			if(strm != null){
				try{
					strm.close();
				}
				catch(IOException ioe){
					// nothing here
				}
			}
			if(in != null){
				try{
					in.close();
				}
				catch(IOException ioe){
					// nothing here
				}
			}
		}
		
	}
	
	public ReceivedMessage pollNextReceivedMessage(InetAddress host, String basePath){
		// in here we reconstruct
		Integer nextMessageId = pollNext(host, receivedMessagesPerHost);
		if(nextMessageId == null) return null;
		String messageFileName = 
				MessageUtils.buildReceivedMessageFilePathForRound(new ReceivedMessage(nextMessageId), basePath, host);
		// now load
		FileInputStream strm = null;
		ObjectInputStream in = null;
		try{
			strm = new FileInputStream(messageFileName);
			in = new ObjectInputStream(strm);
			ReceivedMessage reconstructed = (ReceivedMessage)in.readObject();
			return reconstructed;
		}
		catch(Exception e){
			logger.error("@Cesar: Skipped a message <" + nextMessageId + ">since cannot load", e);
			return null;
		}
		finally{
			if(strm != null){
				try{
					strm.close();
				}
				catch(IOException ioe){
					// nothing here
				}
			}
			if(in != null){
				try{
					in.close();
				}
				catch(IOException ioe){
					// nothing here
				}
			}
		}
	}
	
	private Integer pollNext(InetAddress host, 
						Map<InetAddress, ArrayList> map){
		if(!map.containsKey(host)) return null;
		ArrayList target = map.get(host);
		if(target.size() > 0){
			Integer value = (Integer)target.get(0);
			target.remove(0);
			return value;
		}
		return null;
	}
	
	public void addSentMessage(InetAddress host, 
							   int message){
		addToQueue(host, message, sentMessagesPerHost);
	}
	
	public void addReceivedMessage(InetAddress host, 
								   int message){
		addToQueue(host, message, receivedMessagesPerHost);
	}
	
	private void addToQueue(InetAddress host, 
							int message, 
							Map<InetAddress, ArrayList> map){
		if (map.containsKey(host)){
			map.get(host).add(message);
		}
		else{
			ArrayList pq = new ArrayList();
			pq.add(message);
			map.put(host, pq);
			
		}
	}
	
	public void saveRoundToFile(final GossipRound round, 
							    final String basePath, 
							    final InetAddress id){
		if(locksPerHost.get(id) == null) locksPerHost.put(id, new ReentrantLock());
		locksPerHost.get(id).lock();
		String fileName = MessageUtils.buildSentGossipFilePathForRound(round, basePath, id);
		String mapFileName = MessageUtils.buildSentGossipFilePathForMap(basePath, id);
		PrintWriter pr = null;
		ObjectOutputStream out = null;
		FileOutputStream fopt = null;
		try{
			File file = new File(fileName);
			if(!file.getParentFile().exists()) file.getParentFile().mkdirs(); 
			fopt = new FileOutputStream(fileName);
			out = new ObjectOutputStream(fopt);
			out.writeObject(round);
			// also, print and concat the id to a file
			pr = new PrintWriter(new FileWriter(new File(mapFileName), true));
			pr.println(round.getGossipRound());
			logger.debug("@Cesar: Round <" + round.getGossipRound() + "> saved to <" + fileName + ", " + mapFileName + ">");
		}
		catch(Exception ioe){
			logger.error("@Cesar: Exception while saving <" + fileName + ">", ioe);
		}
		finally{
			try{
				if(pr != null) pr.close();
				if(fopt != null) fopt.close();
				if(out != null) out.close();
				if(locksPerHost.get(id).isLocked()) locksPerHost.get(id).unlock();
			}
			catch(IOException ioe){
				// nothing here
			}
		}
	}
	
	public void saveMessageToFile(final ReceivedMessage message, 
								  final String basePath, 
								  final InetAddress id){
		String fileName = MessageUtils.buildReceivedMessageFilePathForRound(message, basePath, id);
		String mapFileName = MessageUtils.buildReceivedMessageFilePathForMap(basePath, id);
		PrintWriter pr = null;
		ObjectOutputStream out = null;
		FileOutputStream fopt = null;
		try{
			File file = new File(fileName);
			if(!file.getParentFile().exists()) file.getParentFile().mkdirs(); 
			fopt = new FileOutputStream(fileName);
			out = new ObjectOutputStream(fopt);
			out.writeObject(message);
			// also, print and concat the id to a file
			pr = new PrintWriter(new FileWriter(new File(mapFileName), true));
			pr.println(message.getMessageRound() +MessageUtils.STATE_FIELD_SEP + message.getWaitForNext());
			logger.debug("@Cesar: Message <" + message.getMessageRound() + "> saved to <" + fileName + ", " + mapFileName + ">");
		}
		catch(Exception ioe){
			logger.error("@Cesar: Exception while saving <" + fileName + ">", ioe);
		}
		finally{
			if(pr != null) pr.close();
		}
		
	}
	
	private void loadRoundsFromFile(String basePath, InetAddress id){
		String mapFileName = MessageUtils.buildSentGossipFilePathForMap(basePath, id);
		BufferedReader brdr = null;
		try{
			File file = new File(mapFileName);
			brdr = new BufferedReader(new FileReader(file));
			String line = null;
			while((line = brdr.readLine()) != null){
				int roundId = Integer.valueOf(line);
				addSentMessage(id, roundId);
			}
			brdr.close();
			logger.info("@Cesar: <" + getSentMessageQueueSize(id) + "> gossip rounds loaded " + 
						 "for host <" + id + "> from <" + mapFileName + ">");
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
	
	private void loadMessagesFromFile(String basePath, InetAddress id){
		String mapFileName = MessageUtils.buildReceivedMessageFilePathForMap(basePath, id);
		BufferedReader brdr = null;
		try{
			File file = new File(mapFileName);
			brdr = new BufferedReader(new FileReader(file));
			String line = null;
			while((line = brdr.readLine()) != null){
				String [] parsed = line.split(MessageUtils.STATE_FIELD_SEP);
				int roundId = Integer.valueOf(parsed[0]);
				addReceivedMessage(id, roundId);
			}
			brdr.close();
			logger.info("@Cesar: <" + getReceivedMessageQueueSize(id) + "> messages loaded " + 
						 "for host <" + id + "> from <" + mapFileName + ">");
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
	
	public void loadSentGossipRounds(String basePath, List<InetAddress> addressList){
		sentMessagesPerHost.clear();
		for(InetAddress address : addressList) loadRoundsFromFile(basePath, address);
	}
	
	public void loadReceivedMessages(String basePath, List<InetAddress> addressList){
		receivedMessagesPerHost.clear();
		for(InetAddress address : addressList) loadMessagesFromFile(basePath, address);
	}
	
}
