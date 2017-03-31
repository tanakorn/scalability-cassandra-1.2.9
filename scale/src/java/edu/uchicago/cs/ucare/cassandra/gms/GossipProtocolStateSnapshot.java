package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import javax.xml.bind.DatatypeConverter;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.TokenMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipProtocolStateSnapshot implements Serializable{

	private static final Logger logger = LoggerFactory.getLogger(GossipProtocolStateSnapshot.class);
	
	private byte[] tokens = null;
	private byte[] tokenMetadata = null;
	
    public static GossipProtocolStateSnapshot buildFromInstance(GossiperStub gossiper){
    	GossipProtocolStateSnapshot snapshot = new GossipProtocolStateSnapshot();
    	if(gossiper.tokenMetadata != null) {
    		synchronized(gossiper.tokenMetadata){
    			ByteArrayOutputStream bos = null;
    			ObjectOutputStream out = null;
    			try{
    				bos = new ByteArrayOutputStream();
        			out = new ObjectOutputStream(bos);
	    			out.writeObject(gossiper.tokenMetadata);
	    			snapshot.tokenMetadata = bos.toByteArray();
    			}
    			catch(IOException ioe){
    				// nothing here?
    			}
    			finally{
    				try{
    					bos.close();
    				}
    				catch(IOException ioe){
    					// nothing
    				}
    				try{
    					out.close();
    				}
    				catch(IOException ioe){
    					// nothing
    				}
    			}
	    	}
    	}
    	if(gossiper.tokens != null) {
    		synchronized(gossiper.tokens){
    			ByteArrayOutputStream bos = null;
    			ObjectOutputStream out = null;
    			try{
    				bos = new ByteArrayOutputStream();
        			out = new ObjectOutputStream(bos);
	    			out.writeObject(gossiper.tokens);
	    			snapshot.tokens = bos.toByteArray();
    			}
    			catch(IOException ioe){
    				// nothing here?
    			}
    			finally{
    				try{
    					bos.close();
    				}
    				catch(IOException ioe){
    					// nothing
    				}
    				try{
    					out.close();
    				}
    				catch(IOException ioe){
    					// nothing
    				}
    			}
	    	}
    	}
		return snapshot;
	}
	
	public static void loadFromSnapshot(GossipProtocolStateSnapshot snapshot, GossiperStub gossiper){
		if(snapshot.tokenMetadata != null && snapshot.tokenMetadata.length > 0) {
			synchronized(gossiper.tokenMetadata){
				ByteArrayInputStream bis = null;
    			ObjectInputStream in = null;
    			try{
    				bis = new ByteArrayInputStream(snapshot.tokenMetadata);
        			in = new ObjectInputStream(bis);
	    			gossiper.tokenMetadata = (TokenMetadata)in.readObject();
    			}
    			catch(IOException ioe){
    				logger.error("@Cesar: error!", ioe);
    			}
    			catch(ClassNotFoundException cnfe){
    				// nothing
    			}
    			finally{
    				try{
    					bis.close();
    				}
    				catch(IOException ioe){
    					// nothing
    				}
    				try{
    					in.close();
    				}
    				catch(IOException ioe){
    					// nothing
    				}
    			}
	    	}
    	}
		if(snapshot.tokens != null && snapshot.tokens.length > 0) {
			if(gossiper.tokens == null) gossiper.tokens = new HashSet<Token>();
	    	synchronized(gossiper.tokens){	 
	    		ByteArrayInputStream bis = null;
    			ObjectInputStream in = null;
    			try{
    				bis = new ByteArrayInputStream(snapshot.tokens);
        			in = new ObjectInputStream(bis);
	    			gossiper.tokens = (HashSet<Token>)in.readObject();
    			}
    			catch(IOException ioe){
    				logger.error("@Cesar: error!", ioe);
    			}
    			catch(ClassNotFoundException cnfe){
    				// nothing
    			}
    			finally{
    				try{
    					bis.close();
    				}
    				catch(IOException ioe){
    					// nothing
    				}
    				try{
    					in.close();
    				}
    				catch(IOException ioe){
    					// nothing
    				}
    			}
	    	}
    	}
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
	
	public static void seriliazeToFile(final GossipProtocolStateSnapshot snapshot, 
									   final String filePath,
									   final InetAddress id,
									   final Float time, 
									   final String methodName,
									   final GossipProtocolStateSnaphotManager manager,
									   final String messageId){
		new Thread(){
			@Override
			public void run(){
				ObjectOutputStream out = null;
				FileOutputStream fopt = null;
			  	try{
					String hrSerialized = messageId;
					BigInteger hashed = hashId(hrSerialized);
					String fileName = MessageUtils.buildStateFilePathForFile(filePath, id, methodName, hashed);
					// take the precaution to create dir if needed
					File targetFile = new File(fileName);
					if(!targetFile.getParentFile().exists()) targetFile.getParentFile().mkdirs();
					fopt = new FileOutputStream(targetFile);
					out = new ObjectOutputStream(fopt);
					out.writeObject(snapshot);
					if(logger.isDebugEnabled()) logger.debug("@Cesar: serialized <" + fileName + ">");
					// also, save in map file
					manager.storeInFile(filePath, hashed, time, id, methodName, messageId);
			  	}
			  	catch(IOException ioe){
			  		logger.error("@Cesar: Exception on serialization", ioe);
			  	}
			  	finally{
			  		try{
			  			if(out != null) out.close();
			  		}
			  		catch(IOException ioe){
			  			// nothing
			  		}
			  		try{
			  			if(fopt != null) fopt.close();
			  		}
			  		catch(IOException ioe){
			  			// nothing
			  		}
			  		
				}
			}
		}.start();
		
	}
	
	public static GossipProtocolStateSnapshot loadFromFile(String filePath, InetAddress id, String methodName, BigInteger hashValue){
		FileInputStream strm = null;
		ObjectInputStream in = null;
		try{
			// retrieve
			String sourceFileName = MessageUtils.buildStateFilePathForFile(filePath, id, methodName, hashValue);
			// reconstruct
			strm = new FileInputStream(sourceFileName);
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
			if(strm != null){
				try{
					strm.close();
				}
				catch(IOException ioe){
					// nothing here
				}
			}
			if(in != null){
				try{
					in.close();
				}
				catch(IOException ioe){
					// nothing here
				}
			}
		}
	}
	
}
