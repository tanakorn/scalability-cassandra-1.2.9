package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipProtocolStateSnaphotManager {

	private static final Logger logger = LoggerFactory.getLogger(GossipProtocolStateSnaphotManager.class);
	
	private Map<InetAddress, HashMap<BigInteger, Float>> recordedProcessingTimePerNode = 
			new HashMap<InetAddress, HashMap<BigInteger, Float>>();
	
	public Float lookUpTime(InetAddress id, BigInteger lookedUp){
		return recordedProcessingTimePerNode.get(id).get(lookedUp);
	}
	
	public static String buildMessageIdentifier(Object... elements){
		StringBuilder bld = new StringBuilder();
		for(Object o : elements) bld.append(o).append(",");
		if(bld.length() > 0) bld.deleteCharAt(bld.length() - 1);
		return bld.toString();
	}
	
	public void storeInFile(String filePath, 
							BigInteger hash, 
							Float elapsedTime, 
							InetAddress id,
							String methodName){
		PrintWriter writer = null;
	  	try{
			// take the precaution to create dir if needed
			File directory = new File(filePath);
			if(!directory.exists()) directory.mkdirs();
			String destinationFileName = MessageUtils.buildStateFilePath(filePath, id, methodName);
			// append mode
			writer = new PrintWriter(new BufferedWriter(new FileWriter(destinationFileName, true)));
			writer.println(hash + MessageUtils.STATE_FIELD_SEP + elapsedTime);
			if(logger.isDebugEnabled()) logger.debug("@Cesar: stored <" + hash + "> in <" + destinationFileName + ">");
	  	}
	  	catch(IOException ioe){
	  		logger.error("@Cesar: Exception while storing hashed values", ioe);
	  	}
	  	finally{
	  		if(writer != null) writer.close();
		}
	}
	
	private void loadFromFile(String filePath, InetAddress id, String methodName){
		FileReader fr = null;
		BufferedReader bfr = null;
		try{
			String fileName = MessageUtils.buildStateFilePath(filePath, id, methodName);
			fr = new FileReader(new File(fileName));
			bfr = new BufferedReader(fr);
			String line = null;
			while((line = bfr.readLine()) != null){
				// read line by line and parse. Its going
				// to be FIELD_SEP separated. 
				String [] vals = line.split(MessageUtils.STATE_FIELD_SEP);
				// idFIELD_SEPtime
				BigInteger mId = new BigInteger(vals[0]);
				float time = Float.parseFloat(vals[1]);
				// i will record the max time
				HashMap<BigInteger, Float> mp = recordedProcessingTimePerNode.get(id);
				if(mp == null){
					mp = new HashMap<BigInteger, Float>();
					mp.put(mId, time);
					recordedProcessingTimePerNode.put(id, mp);
				}
				else{
					mp.put(mId, time);
				}
			}
			logger.info("@Cesar: Loaded <" + recordedProcessingTimePerNode.get(id).size() + 
						"> values to replay from <" + 
						fileName + "> on node <" + id + ">");
		}
		catch(IOException ioe){
			logger.error("@Cesar: Could not load initial message list!", ioe);
		}
		finally{
			try{
    			if(fr != null) fr.close();
    			if(bfr != null) bfr.close();
			}
			catch(IOException ioe){
				logger.error("@Cesar: Could not close streams!", ioe);
			}
		}
	}
	
	public void loadStatesFromFiles(String filePath, List<InetAddress> hosts, String methodName){
		recordedProcessingTimePerNode.clear();
		for(InetAddress host : hosts) loadFromFile(filePath, host, methodName);
	}
	
}
