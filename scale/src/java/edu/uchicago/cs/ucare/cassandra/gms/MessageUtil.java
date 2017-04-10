package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.File;
import java.net.InetAddress;

import edu.uchicago.cs.ucare.cassandra.gms.ReceivedMessageManager.ReceivedMessage;


public class MessageUtil {

	public static final String STATE_FIELD_SEP = ",";
	
	private static final String RECEIVED_MESSAGE_MAP = "message-mapping";
	private static final String MESSAGE_RECEIVED = "message-queue";
	private static final String TIME = "time";
	
	public static String buildTimeFileName(String basePath){
		return basePath + File.separator + TIME;
    }
	
	public static String buildReceivedMessageFilePath(String basePath, InetAddress host){
		return basePath + File.separator + host + File.separator + MESSAGE_RECEIVED;
	}
	
	public static String buildReceivedMessageFilePathForRound(ReceivedMessage message, String basePath, InetAddress host){
		return buildReceivedMessageFilePath(basePath, host) + File.separator + message.getMessageRound();
	}
	
	public static String buildReceivedMessageFilePathForMap(String basePath, InetAddress host){
		return buildReceivedMessageFilePath(basePath, host) + File.separator + RECEIVED_MESSAGE_MAP;
			   
	}
	
}
