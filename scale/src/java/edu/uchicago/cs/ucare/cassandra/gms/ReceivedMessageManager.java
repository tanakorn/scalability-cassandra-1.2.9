package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ReceivedMessageManager{

	private static Logger logger = LoggerFactory.getLogger(ReceivedMessageManager.class);
	
	private Queue<Integer> msgQueue = null;
	private String messageBasePath = null;
	
	public ReceivedMessageManager(String messageBasePath){
		if(messageBasePath != null){
			this.messageBasePath = messageBasePath;
			msgQueue = new LinkedBlockingQueue<Integer>();
		}
	}
	
	public void saveReceivedMessageToFile(final ReceivedMessage message){
		String fileName = MessageUtil.buildReceivedMessageFilePathForRound(message, messageBasePath);
		String mapFileName = MessageUtil.buildReceivedMessageFilePathForMap(messageBasePath);
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
			pr.println(message.getMessageRound() + MessageUtil.STATE_FIELD_SEP + message.getMessageId());
			if(logger.isDebugEnabled()) logger.debug("@Cesar: Message <" + message.getMessageRound() + "> saved to <" + fileName + ", " + mapFileName + ">");
		}
		catch(Exception ioe){
			logger.error("@Cesar: Exception while saving <" + fileName + ">", ioe);
		}
		finally{
			if(pr != null) pr.close();
		}

	}
	
	public void loadReceivedMessagesFromFile(){
		String mapFileName = MessageUtil.buildReceivedMessageFilePathForMap(messageBasePath);
		BufferedReader brdr = null;
		try{
			File file = new File(mapFileName);
			brdr = new BufferedReader(new FileReader(file));
			String line = null;
			Set<ReceivedMessage.MessageLoader> orderedMessages = new TreeSet<ReceivedMessage.MessageLoader>(new ReceivedMessage.MessageLoaderComparator());
			while((line = brdr.readLine()) != null){
				String [] parsed = line.split(MessageUtil.STATE_FIELD_SEP);
				int roundId = Integer.valueOf(parsed[0]);
				long messageId = Long.valueOf(parsed[1]);
				ReceivedMessage.MessageLoader lrd = new ReceivedMessage.MessageLoader(roundId, messageId);
				orderedMessages.add(lrd);
			}
			int old = 0;
			for(ReceivedMessage.MessageLoader ordered : orderedMessages){
				if(old > 0 && old >= ordered.messageRound){
					logger.error("@Cesar: Error while loading?");
				}
				msgQueue.add(ordered.messageRound);
				old = ordered.messageRound;
			}
			brdr.close();
			logger.info("@Cesar: <" + msgQueue.size() + "> messages loaded  from <" + mapFileName + ">");
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
	
	public ReceivedMessage pollNextReceivedMessage(){
		// in here we reconstruct
		Integer nextMessageId = msgQueue.poll();
		if(nextMessageId == null) return null;
		String messageFileName = MessageUtil.buildReceivedMessageFilePathForRound(
				new ReceivedMessage(nextMessageId), 
				messageBasePath);
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
	
	public static class ReceivedMessage implements Serializable{
		
		private static int ROUND = 0;
		
		private int messageRound = 0;
		private MessageIn<?> messageIn = null;
		private long messageId = 0; 
		
		public ReceivedMessage(){
			// nothing here
		}
		
		public ReceivedMessage(MessageIn<?> messageIn, long messageId){
			this.messageId = messageId;
			this.messageIn = messageIn;
			messageRound = ROUND;
			++ROUND;
		}

		public ReceivedMessage(int messageRound){
			this.messageRound = messageRound;
		}
		
		public int getMessageRound() {
			return messageRound;
		}

		public void setMessageRound(int messageRound) {
			this.messageRound = messageRound;
		}

		public MessageIn<?> getMessageIn() {
			return messageIn;
		}

		public void setMessageIn(MessageIn<?> messageIn) {
			this.messageIn = messageIn;
		}

		public long getMessageId() {
			return messageId;
		}

		public void setMessageId(int messageId) {
			this.messageId = messageId;
		}
		
		
		public static class MessageLoader{
			private int messageRound = 0;
			private long messageId = 0; 
			
			public MessageLoader(int messageRound, long messageId){
				this.messageRound = messageRound;
				this.messageId = messageId;
			}
		}
		
		public static class MessageLoaderComparator implements Comparator<MessageLoader>{
			@Override
			public int compare(MessageLoader o1, MessageLoader o2) {
				if(o1.messageId != o1.messageId){
					return (int)(o1.messageId - o2.messageId);
				}
				else{
					return o1.messageRound - o2.messageRound;
				}
			}
			
		}
		
	}
	
}
