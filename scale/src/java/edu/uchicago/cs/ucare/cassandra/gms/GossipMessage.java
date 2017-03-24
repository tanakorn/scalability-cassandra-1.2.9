package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.Serializable;
import java.net.InetAddress;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;

public class GossipMessage implements Serializable{

	public static enum GossipType{
		TO_LIVE_MEMBER,
		TO_UNREACHABLE_MEMBER,
		TO_SEED
	}
	
	private GossipType type = GossipType.TO_LIVE_MEMBER;
	private InetAddress toWho = null;
	private MessageIn<GossipDigestSyn> message = null;
	
	public GossipType getType() {
		return type;
	}
	
	public void setType(GossipType type) {
		this.type = type;
	}
	
	public InetAddress getToWho() {
		return toWho;
	}
	
	public void setToWho(InetAddress toWho) {
		this.toWho = toWho;
	}
	
	public MessageIn<GossipDigestSyn> getMessage() {
		return message;
	}
	
	public void setMessage(MessageIn<GossipDigestSyn> message) {
		this.message = message;
	}
	
	public static GossipMessage build(GossipType type, MessageIn<GossipDigestSyn> message, InetAddress toWho){
		GossipMessage msg = new GossipMessage();
		msg.setMessage(message);
		msg.setToWho(toWho);
		msg.setType(type);
		return msg;
	}
	
}
