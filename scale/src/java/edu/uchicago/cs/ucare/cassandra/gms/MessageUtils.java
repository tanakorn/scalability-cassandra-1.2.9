package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.File;
import java.net.InetAddress;

public class MessageUtils {

	private static final String GOSSIP_SENT = "gossip-sent";
	private static final String GOSSIP_RECEIVED = "gossip-received";
	
	public static String buildSentGossipFilePath(String basePath, InetAddress id){
		return basePath + 
			   File.separator + 
			   id.toString().substring(1, id.toString().length()) +
			   GOSSIP_SENT;
	}
	
	public static String buildReceivedGossipFilePath(String basePath, InetAddress id){
		return basePath + 
			   File.separator + 
			   id.toString().substring(1, id.toString().length()) +
			   GOSSIP_RECEIVED;
	}
	
	public static String buildSentGossipFilePathForRound(GossipRound round, String basePath, InetAddress id){
		return buildSentGossipFilePath(basePath, id) + 
			   File.separator + 
			   round.getGossipRound();
			   
	}
	
}
