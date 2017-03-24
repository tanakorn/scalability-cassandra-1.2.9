package org.apache.cassandra.gms;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.DatatypeConverter;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageRecordingUtils {
	
	private static final Logger logger = LoggerFactory.getLogger(MessageRecordingUtils.class);
	private static AtomicInteger SENT_COUNTER = new AtomicInteger(0);
	private static AtomicInteger RECEIVE_COUNTER = new AtomicInteger(0);
	
	// how messages are separated when written
    public static String MESSAGE_SEPARATOR = "xyz@@xyz";
    
    // load an initial list of messages to replay
    private final Queue<SerializedGossipMessage> serializedSentMessages = 
    		new PriorityBlockingQueue<SerializedGossipMessage>(10, new SerializedGossipMessageCompparator());
    private final Queue<SerializedGossipMessage> serializedReceivedMessages = 
    		new PriorityBlockingQueue<SerializedGossipMessage>(10, new SerializedGossipMessageCompparator());
    
    public static String printReceivedMessage(String id, MessageIn message, String targetFileName) throws Exception{
    	return id + MESSAGE_SEPARATOR + SerializedGossipMessage.messageToString(message, id, message.from, targetFileName);
    }
    
    public static String printSentMessage(String id, MessageOut message, InetAddress to, String targetFileName) throws Exception{
    	return id + MESSAGE_SEPARATOR + SerializedGossipMessage.messageToString(message, id, to, targetFileName);
    }
    
    private boolean isSentSerializedMessageListLoaded = false;
    private boolean isReceivedSerializedMessageListLoaded = false;
    private Boolean recordingEnabled = null;
    private Boolean replayEnabled = null;
    private String receivedMessagesFileName = null;
    private String sentMessagesFileName = null;
    
    
    public SerializedGossipMessage pollNextSentMessage(){
    	return serializedSentMessages.poll();
    }
    
    public SerializedGossipMessage pollNextReceivedMessage(){
    	return serializedReceivedMessages.poll();
    }
    
    public int sentSize(){
    	return serializedSentMessages.size();
    }
    
    public int receivedSize(){
    	return serializedReceivedMessages.size();
    }
    
    public boolean isRecordingEnabled(){
    	if(recordingEnabled == null) recordingEnabled = Boolean.parseBoolean(System.getProperty("edu.uchicago.ucare.sck.recordSentMessages", "FALSE"));
    	return recordingEnabled;
    }
    
    public boolean isReplayEnabled(){
    	if(replayEnabled == null) replayEnabled = Boolean.parseBoolean(System.getProperty("edu.uchicago.ucare.sck.replayRecordedMessages", "FALSE"));
    	return replayEnabled;
    }
    
    public void loadInitialSentMessages() throws Exception{
    	if(!isSentSerializedMessageListLoaded)
    		isSentSerializedMessageListLoaded = loadInitialMessageList(getSentMessageIndexedDirectory(), serializedSentMessages);
    }
    
    public void loadInitialReceivedMessages() throws Exception{
    	if(!isReceivedSerializedMessageListLoaded)
    		isReceivedSerializedMessageListLoaded = loadInitialMessageList(getReceivedMessageIndexedDirectory(), serializedReceivedMessages);
    }
    
    // utility method to initially load the list of messages
    private boolean loadInitialMessageList(String fileName, Queue<SerializedGossipMessage> queue) throws Exception{
    	queue.clear();
    	FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		try{
			// for each file in this dir
			File theDir = new File(fileName);
			for(File target : theDir.listFiles()){
				fileReader = new FileReader(target);
				bufferedReader = new BufferedReader(fileReader);
				// each file has one line
				SerializedGossipMessage msg = SerializedGossipMessage.messageFromString(bufferedReader.readLine());
				queue.add(msg);
				fileReader.close();
				bufferedReader.close();
			}
			return true;
		}
		finally{
			if(fileReader != null) fileReader.close();
			if(bufferedReader != null) bufferedReader.close();
			logger.info("@Cesar: <" + queue.size() + "> messages loaded from <" + 
						fileName + ">");
		}
    	
    }
    
    public String getSentMessageFileName(){
    	if(sentMessagesFileName == null){
	    	String serializationFilePrefix = System.getProperty("edu.uchicago.ucare.sck.serializationFilePrefix", null);
	    	String nodeName = FBUtilities.getBroadcastAddress().toString();
	    	if(nodeName.startsWith("/")) nodeName = nodeName.substring(1, nodeName.length());
	    	sentMessagesFileName = serializationFilePrefix + File.separator + nodeName + "-sent";
    	}
    	return sentMessagesFileName;
    }
    
    public String getReceivedMessageFileName(){
    	if(receivedMessagesFileName == null){
	    	String serializationFilePrefix = System.getProperty("edu.uchicago.ucare.sck.serializationFilePrefix", null);
	    	String nodeName = FBUtilities.getBroadcastAddress().toString();
	    	if(nodeName.startsWith("/")) nodeName = nodeName.substring(1, nodeName.length());
	    	receivedMessagesFileName = serializationFilePrefix + File.separator + nodeName + "-received";
    	}
    	return receivedMessagesFileName;
    }
    
    public String getSentMessageIndexedDirectory(){
    	String serializationFilePrefix = System.getProperty("edu.uchicago.ucare.sck.serializationFilePrefix", null);
    	String nodeName = FBUtilities.getBroadcastAddress().toString();
    	if(nodeName.startsWith("/")) nodeName = nodeName.substring(1, nodeName.length());
    	return serializationFilePrefix + File.separator + nodeName + "-sent";
    }
    
    public String getSentMessageIndexedFileName(){
    	return getSentMessageIndexedDirectory() + File.separator + SENT_COUNTER.getAndIncrement();
    }
    
    public String getReceivedMessageIndexedDirectory(){
    	String serializationFilePrefix = System.getProperty("edu.uchicago.ucare.sck.serializationFilePrefix", null);
    	String nodeName = FBUtilities.getBroadcastAddress().toString();
    	if(nodeName.startsWith("/")) nodeName = nodeName.substring(1, nodeName.length());
    	return serializationFilePrefix + File.separator + nodeName + "-received";
    }
    
    public String getReceivedMessageIndexedFileName(){
    	return getReceivedMessageIndexedDirectory() + File.separator + RECEIVE_COUNTER.getAndIncrement();
    }
    
    public static int getIdFromFileName(String fileName){
    	return Integer.valueOf(fileName.substring(fileName.lastIndexOf(File.separator) + 1, fileName.length()));
    }
    
	public static class SerializedGossipMessage implements Serializable{

    	private MessageOut retrievedMessageSent = null;
    	private MessageIn  retrievedMessageReceived = null;
		private String retrievedId = null;
		private InetAddress retrievedDestinatary = null;
		private int messageRecordingId = 0;

		public static SerializedGossipMessage messageFromString(String line) throws Exception{
			String[] data = line.split(MESSAGE_SEPARATOR);
			ByteArrayInputStream strm = null;
			ObjectInputStream in = null;
			SerializedGossipMessage message = null;
			try{
				// reconstruct
				String id = data[0];
				byte [] decodedMessage = DatatypeConverter.parseBase64Binary(data[1]);
				strm = new ByteArrayInputStream(decodedMessage);
				in = new ObjectInputStream(strm);
				message = (SerializedGossipMessage)in.readObject();
				return message;
			}
			finally{
				if(strm != null) strm.close();
				if(in != null) in.close();
			}
		}
		
		public static String messageToString(MessageOut msg, String id, InetAddress destinatary, String targetFileName) throws Exception{
			SerializedGossipMessage message = new SerializedGossipMessage();
			message.retrievedId = id;
			message.retrievedDestinatary = destinatary;
			message.retrievedMessageSent = msg;
			message.messageRecordingId = MessageRecordingUtils.getIdFromFileName(targetFileName);
			ByteArrayOutputStream strm = null;
	    	ObjectOutputStream out = null;
	    	try{
				strm = new ByteArrayOutputStream();
		    	out = new ObjectOutputStream(strm);
		    	out.writeObject(message);
		    	return DatatypeConverter.printBase64Binary(strm.toByteArray());
	    	}
	    	finally{
	    		if(strm != null) strm.close();
				if(out != null) out.close();
	    	}
		}
		
		public static String messageToString(MessageIn msg, String id, InetAddress from, String targetFileName) throws Exception{
			SerializedGossipMessage message = new SerializedGossipMessage();
			message.retrievedId = id;
			message.retrievedDestinatary = from;
			message.retrievedMessageReceived = msg;
			message.messageRecordingId = MessageRecordingUtils.getIdFromFileName(targetFileName);
			ByteArrayOutputStream strm = null;
	    	ObjectOutputStream out = null;
	    	try{
				strm = new ByteArrayOutputStream();
		    	out = new ObjectOutputStream(strm);
		    	out.writeObject(message);
		    	return DatatypeConverter.printBase64Binary(strm.toByteArray());
	    	}
	    	finally{
	    		if(strm != null) strm.close();
				if(out != null) out.close();
	    	}
		}

		public MessageOut getRetrievedMessageSent() {
			return retrievedMessageSent;
		}

		public MessageIn getRetrievedMessageReceived() {
			return retrievedMessageReceived;
		}

		public String getRetrievedId() {
			return retrievedId;
		}

		public InetAddress getRetrievedDestinatary() {
			return retrievedDestinatary;
		}	
		
    }
	
	public static class SerializedGossipMessageCompparator implements Comparator<SerializedGossipMessage>{
		@Override
		public int compare(SerializedGossipMessage o1, SerializedGossipMessage o2) {
			return o1.messageRecordingId - o2.messageRecordingId;
		}
		
	}

}
