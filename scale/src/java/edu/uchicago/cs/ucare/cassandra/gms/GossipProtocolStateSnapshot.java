package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import javax.xml.bind.DatatypeConverter;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipProtocolStateSnapshot implements Serializable{

	private static final Logger logger = LoggerFactory.getLogger(GossipProtocolStateSnapshot.class);
	
	private Map<InetAddress, EndpointState> endpointStateMap = new HashMap<InetAddress, EndpointState>();
    private Set<InetAddress> liveEndpoints = new HashSet<InetAddress>();
    private Map<InetAddress, Long> unreachableEndpoints = new HashMap<InetAddress, Long>();
	
    public static GossipProtocolStateSnapshot buildFromInstance(Gossiper gossiper){
    	GossipProtocolStateSnapshot snapshot = new GossipProtocolStateSnapshot();
		snapshot.endpointStateMap.clear(); snapshot.endpointStateMap.putAll(gossiper.getEndpointStateMap());
		snapshot.liveEndpoints.clear(); snapshot.liveEndpoints.addAll(gossiper.getLiveEndpoints());
		snapshot.unreachableEndpoints.clear(); snapshot.unreachableEndpoints.putAll(gossiper.getUnreachableEndpoints());
		return snapshot;
	}
	
	public static void loadFromSnapshot(GossipProtocolStateSnapshot snapshot, Gossiper gossiper){
		gossiper.getEndpointStateMap().clear(); gossiper.getEndpointStateMap().putAll(snapshot.endpointStateMap);
		gossiper.getLiveEndpoints().clear(); gossiper.getLiveEndpoints().addAll(snapshot.liveEndpoints);
		gossiper.getUnreachableEndpoints().clear(); gossiper.getUnreachableEndpoints().putAll(snapshot.unreachableEndpoints);
	}
    
	private static String seriliazeToString(GossipProtocolStateSnapshot snapshot){
		ByteArrayOutputStream strm = null;
	  	ObjectOutputStream out = null;
	  	try{
		  	strm = new ByteArrayOutputStream();
			out = new ObjectOutputStream(strm);
			out.writeObject(snapshot);
			String serialized = DatatypeConverter.printBase64Binary(strm.toByteArray());
			return serialized;
	  	}
	  	catch(IOException ioe){
	  		logger.error("@Cesar: Exception on serialization", ioe);
	  		return null;
	  	}
	  	finally{
	  		try{
				if(strm != null) strm.close();
				if(out != null) out.close();
	  		}
	  		catch(IOException ioe){
	  			logger.error("@Cesar: Could not close streams!", ioe);
	  		}
		}
	}
	
	public static BigInteger hashId(String input){
		MessageDigest digest = null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
		try{
			digest = MessageDigest.getInstance("MD5");
			// write
			dos.writeUTF(input);
			// get bytes
			byte[] hashBytes = digest.digest(baos.toByteArray());
			// and ready
			return new BigInteger(hashBytes);
		}
		catch(NoSuchAlgorithmException nsae){
			logger.error("@Cesar: MD5 does not exists!", nsae);
			return null;
		}
		catch(IOException ioe){
			logger.error("@Cesar: IOException while hashing", ioe);
			return null;
		}
	}
	
	public static void seriliazeToFile(GossipProtocolStateSnapshot snapshot, 
									   String filePath,
									   InetAddress id,
									   Float time, 
									   String methodName,
									   GossipProtocolStateSnaphotManager manager,
									   String messageId){
	  	PrintWriter writer = null;
	  	try{
			String serialized = seriliazeToString(snapshot);
			String hrSerialized = messageId;
			BigInteger hashed = hashId(hrSerialized);
			String fileName = MessageUtils.buildStateFilePathForFile(filePath, id, methodName, hashed);
			// take the precaution to create dir if needed
			File targetFile = new File(fileName);
			if(!targetFile.getParentFile().exists()) targetFile.getParentFile().mkdirs();
			writer = new PrintWriter(targetFile);
			writer.println(serialized);
			if(logger.isDebugEnabled()) logger.debug("@Cesar: serialized <" + fileName + ">");
			// also, save in map file
			manager.storeInFile(filePath, hashed, time, id, methodName);
	  	}
	  	catch(IOException ioe){
	  		logger.error("@Cesar: Exception on serialization", ioe);
	  	}
	  	finally{
	  		if(writer != null) writer.close();
		}
	}
	
	public static GossipProtocolStateSnapshot loadFromFile(String filePath, InetAddress id, String methodName, BigInteger hashValue){
		ByteArrayInputStream strm = null;
		ObjectInputStream in = null;
		BufferedReader bfr = null;
		FileReader fr = null;
		try{
			// retrieve
			String sourceFileName = MessageUtils.buildStateFilePathForFile(filePath, id, methodName, hashValue);
			fr = new FileReader(new File(sourceFileName));
			bfr = new BufferedReader(fr);
			String target = bfr.readLine();
			// reconstruct
			byte [] decodedMessage = DatatypeConverter.parseBase64Binary(target);
			strm = new ByteArrayInputStream(decodedMessage);
			in = new ObjectInputStream(strm);
			GossipProtocolStateSnapshot reconstructed = (GossipProtocolStateSnapshot)in.readObject();
			// done
			return reconstructed;
		}
		catch(IOException ioe){
			logger.error("@Cesar: Cannot deserialize!!", ioe);
			return null;
		}
		catch(ClassNotFoundException cnfe){
			logger.error("@Cesar: ClassNotFound????", cnfe);
			return null;
		}
		finally{
			try{
				if(strm != null) strm.close();
				if(in != null) in.close();
				if(fr != null) fr.close();
				if(bfr != null) bfr.close();
			}
			catch(IOException ioe){
				// nothing to do here, dont care
	  			logger.error("@Cesar: Could not close streams!", ioe);
			}
		}
	}
	
}
