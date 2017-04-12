package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.File;

import edu.uchicago.cs.ucare.cassandra.gms.ReceivedMessageManager.ReceivedMessage;


public class MessageUtil {

	public static final String STATE_FIELD_SEP = ",";
	
	private static final String RECEIVED_MESSAGE_MAP = "message-mapping";
	private static final String RECEIVED_MESSAGE__DEP_MAP = "message-dependency-per-host";
	private static final String MESSAGE_RECEIVED = "message-queue";
	private static final String MESSAGE_PROCESSING_TIME = "message-processing-time";
	private static final String TIME = "time";
	
	public static String buildTimeFileName(String basePath){
		return basePath + File.separator + TIME;
    }
	
	public static String buildReceivedMessageFilePath(String basePath){
		return basePath + File.separator + MESSAGE_RECEIVED;
	}
	
	public static String buildReceivedMessageFilePathForRound(ReceivedMessage message, String basePath){
		return buildReceivedMessageFilePath(basePath) + File.separator + message.getMessageRound();
	}
	
	public static String buildReceivedMessageFilePathForMap(String basePath){
		return buildReceivedMessageFilePath(basePath) + File.separator + RECEIVED_MESSAGE_MAP;
			   
	}
	
	public static String buildReceivedMessageFilePathForDependencyMap(String basePath){
		return buildReceivedMessageFilePath(basePath) + File.separator + RECEIVED_MESSAGE__DEP_MAP;
			   
	}
	
	public static String buildMessageProcessingTimeMap(String basePath){
		return buildReceivedMessageFilePath(basePath) + File.separator + MESSAGE_PROCESSING_TIME;
			   
	}
}
