package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Comparator;

import javax.xml.bind.DatatypeConverter;

import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceivedMessage implements Serializable{

private static final Logger logger = LoggerFactory.getLogger(GossipRound.class);
	
	private int messageRound = 0;
	private MessageIn<?> messageIn = null;
	private long waitForNext = 0L;
	
	public ReceivedMessage(int messageRound){
		this.messageRound = messageRound;
	}
	
	public MessageIn<?> getMessageIn() {
		return messageIn;
	}
	public void setMessageIn(MessageIn<?> messageIn) {
		this.messageIn = messageIn;
	}
	public int getMessageRound() {
		return messageRound;
	}
	
	public long getWaitForNext() {
		return waitForNext;
	}

	public void setWaitForNext(long waitForNext) {
		this.waitForNext = waitForNext;
	}

	public static String messageToString(ReceivedMessage message) throws Exception{
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
	
	public static ReceivedMessage messageFromString(String line) throws Exception{
		ByteArrayInputStream strm = null;
		ObjectInputStream in = null;
		ReceivedMessage message = null;
		try{
			byte [] decodedMessage = DatatypeConverter.parseBase64Binary(line);
			strm = new ByteArrayInputStream(decodedMessage);
			in = new ObjectInputStream(strm);
			message = (ReceivedMessage)in.readObject();
			return message;
		}
		finally{
			if(strm != null) strm.close();
			if(in != null) in.close();
		}
	}
	
	public static class ReceivedMessageComparator implements Comparator<ReceivedMessage>{
		@Override
		public int compare(ReceivedMessage o1, ReceivedMessage o2) {
			return o1.messageRound - o2.messageRound;
		}
	}
	
	
}
