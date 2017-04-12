package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageManager {

	public static final MessageManager instance = new MessageManager();
	private static Logger logger = LoggerFactory.getLogger(MessageManager.class);
	
	private boolean isReplayEnabled = false;
	private ReceivedMessageManager receivedMessageManager = null;
	
	
	public void initMessageManager(boolean isReplayEnabled, String messagingBasePath){
		this.isReplayEnabled = isReplayEnabled;
		if(messagingBasePath != null){
			receivedMessageManager = new ReceivedMessageManager(messagingBasePath);
			if(this.isReplayEnabled) receivedMessageManager.loadReceivedMessagesFromFile();
		}
		else{
			logger.error("@Cesar: Received message path is null!");
		}
	}


	public ReceivedMessageManager getReceivedMessageManager() {
		return receivedMessageManager;
	}
	
	
	
	

	
	
	
	
}
