package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;

import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimePreservingService {

	private static final Logger logger = LoggerFactory.getLogger(TimePreservingService.class);
	
	private static long baseTimeStamp = 0L;
    private static long relativeTimeStamp = 0L;
    
	public static void setRelativeTimeStamp(){
    	relativeTimeStamp = System.currentTimeMillis();
    }
    
    public static long getCurrentTimeMillis(boolean isReplayEnabled){
    	if(false && isReplayEnabled ){
    		// is this is enabled, then we have to return time passed relative
    		// to out beggining
    		long elapsed = System.currentTimeMillis() - relativeTimeStamp;
    		return elapsed > 0? baseTimeStamp + elapsed : baseTimeStamp;
    	}
    	else{
    		return System.currentTimeMillis();
    	}
    	
    }
    
    
    public static void saveInitialTime(String basePath){
    	PrintWriter pr = null;
    	long now = System.currentTimeMillis();
    	try{
            pr = new PrintWriter(new File(MessageUtils.buildTimeFileName(basePath)));
            pr.println(now);
            logger.debug("@Cesar: Saved timestamp <" + now + ">");
        }
        catch(IOException e){
            logger.error("Exception, cannot serialize message", e);
        }
    	finally{
    		if(pr != null) pr.close();
    	}
    }
    
    public static void loadInitialTime(String basePath){
    	BufferedReader brdr = null;
    	try{
    		brdr = new BufferedReader(new FileReader(new File(MessageUtils.buildTimeFileName(basePath))));
            String time = brdr.readLine();
            baseTimeStamp = Long.valueOf(time);
            logger.debug("@Cesar: Timestamp loaded from timestamp <" + MessageUtils.buildTimeFileName(basePath) + ">");
        }
        catch(IOException e){
            logger.error("Exception, cannot serialize message", e);
        }
    	finally{
    		try{
    			if(brdr != null) brdr.close();
    		}
    		catch(IOException ioe){
    			// nothing here
    		}
    	}
    }
}

