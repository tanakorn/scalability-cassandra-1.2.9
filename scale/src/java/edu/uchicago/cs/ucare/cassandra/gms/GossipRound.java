package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Comparator;
import javax.xml.bind.DatatypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class GossipRound implements Serializable{

	private static final Logger logger = LoggerFactory.getLogger(GossipRound.class);
	
	private int gossipRound = 0;
	private GossipMessage toLiveMember = null;
	private GossipMessage toUnreachableMember = null;
	private GossipMessage toSeed = null;
	
	public GossipRound(int round){
		gossipRound = round;
	}
	
	public int getGossipRound() {
		return gossipRound;
	}
	
	public GossipMessage getToLiveMember() {
		return toLiveMember;
	}
	
	public void setToLiveMember(GossipMessage toLiveMember) {
		this.toLiveMember = toLiveMember;
	}
	
	public GossipMessage getToUnreachableMember() {
		return toUnreachableMember;
	}
	
	public void setToUnreachableMember(GossipMessage toUnreachableMember) {
		this.toUnreachableMember = toUnreachableMember;
	}
	
	public GossipMessage getToSeed() {
		return toSeed;
	}
	
	public void setToSeed(GossipMessage toSeed) {
		this.toSeed = toSeed;
	}
	
	public static String messageToString(GossipRound round) throws Exception{
		ByteArrayOutputStream strm = null;
    	ObjectOutputStream out = null;
    	try{
			strm = new ByteArrayOutputStream();
	    	out = new ObjectOutputStream(strm);
	    	out.writeObject(round);
	    	return DatatypeConverter.printBase64Binary(strm.toByteArray());
    	}
    	finally{
    		if(strm != null) strm.close();
			if(out != null) out.close();
    	}
	}
	
	public static GossipRound messageFromString(String line) throws Exception{
		ByteArrayInputStream strm = null;
		ObjectInputStream in = null;
		GossipRound round = null;
		try{
			byte [] decodedMessage = DatatypeConverter.parseBase64Binary(line);
			strm = new ByteArrayInputStream(decodedMessage);
			in = new ObjectInputStream(strm);
			round = (GossipRound)in.readObject();
			return round;
		}
		finally{
			if(strm != null) strm.close();
			if(in != null) in.close();
		}
	}
	
	public static class GossipRoundComparator implements Comparator<GossipRound>{
		@Override
		public int compare(GossipRound o1, GossipRound o2) {
			return o1.gossipRound - o2.gossipRound;
		}
	}
	
}
