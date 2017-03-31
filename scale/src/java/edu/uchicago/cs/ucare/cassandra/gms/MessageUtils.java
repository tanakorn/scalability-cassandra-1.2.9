package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.File;
import java.math.BigInteger;
import java.net.InetAddress;

public class MessageUtils {

	private static final String GOSSIP_SENT = "gossip-sent";
	private static final String MESSAGE_RECEIVED = "message-received";
	private static final String TIME = "time";
	public static final String STATE_FILE = "stateProcessingTimeMap";
	public static final String SENT_GOSSIP_MAP = "sentGossipMap";
	public static final String RECEIVED_MESSAGE_MAP = "receivedMessageMap";
	public static final String STATE_FIELD_SEP = ",";
	
	public static String buildTimeFileName(String basePath){
		return basePath + 
			   File.separator + 
			   TIME;
    }
    
	public static String buildStateFilePath(String basePath, InetAddress id, String method){
		return basePath + 
			   File.separator + 
			   id.toString().substring(1, id.toString().length()) +
			   File.separator +
			   method +
			   File.separator +
			   STATE_FILE;
	}
	
	public static String buildStateFilePathForFile(String basePath, InetAddress id, String method, BigInteger hash){
		return basePath + 
			   File.separator + 
			   id.toString().substring(1, id.toString().length()) +
			   File.separator +
			   method +
			   File.separator +
			   hash;
	}
	
	public static String buildSentGossipFilePath(String basePath, InetAddress id){
		return basePath + 
			   File.separator + 
			   id.toString().substring(1, id.toString().length()) +
			   File.separator +
			   GOSSIP_SENT;
	}
	
	public static String buildReceivedMessageFilePath(String basePath, InetAddress id){
		return basePath + 
			   File.separator + 
			   id.toString().substring(1, id.toString().length()) +
			   File.separator +
			   MESSAGE_RECEIVED;
	}
	
	public static String buildReceivedMessageFilePathForRound(ReceivedMessage message, String basePath, InetAddress id){
		return buildReceivedMessageFilePath(basePath, id) + 
			   File.separator + 
			   message.getMessageRound();
			   
	}
	
	public static String buildSentGossipFilePathForRound(GossipRound round, String basePath, InetAddress id){
		return buildSentGossipFilePath(basePath, id) + 
			   File.separator + 
			   round.getGossipRound();
			   
	}
	
	public static String buildSentGossipFilePathForMap(String basePath, InetAddress id){
		return buildSentGossipFilePath(basePath, id) + 
			   File.separator + 
			   SENT_GOSSIP_MAP;
			   
	}
	
	public static String buildReceivedMessageFilePathForMap(String basePath, InetAddress id){
		return buildReceivedMessageFilePath(basePath, id) + 
			   File.separator + 
			   RECEIVED_MESSAGE_MAP;
			   
	}
	
}
