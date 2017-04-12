package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeManager {

	public static final TimeManager instance = new TimeManager();
	private static Logger logger = LoggerFactory.getLogger(TimeManager.class);
	
	private long baseTimeStamp = 0L;
    private long relativeTimeStamp = 0L;
    private boolean isReplayEnabled = false;
    private String baseTimeFilePath = null;
    
    public void initTimeManager(boolean isReplayEnabled, String baseTimeFilePath){
    	this.isReplayEnabled = isReplayEnabled;
    	this.relativeTimeStamp = System.currentTimeMillis();
    	this.baseTimeFilePath = baseTimeFilePath;
    	if(this.isReplayEnabled) {
    		if(this.baseTimeFilePath != null){
    			loadInitialTime();
    		}
    		else{
    			logger.error("@Cesar: Time file path is null");
    		}
    	}
    }
    
    public long getRelativeTimeStamp() {
		return relativeTimeStamp;
	}
    
	public long getCurrentTimeMillisFromBaseTimeStamp(){
    	if(isReplayEnabled ){
    		long elapsed = System.currentTimeMillis() - relativeTimeStamp;
    		return elapsed > 0? baseTimeStamp + elapsed : baseTimeStamp;
    	}
    	else{
    		return System.currentTimeMillis();
    	}
    	
    }
    
	public long getCurrentTimeMillisFromRelativeTimeStamp(long afterHowLong){
    	if(isReplayEnabled ){
    		return relativeTimeStamp + afterHowLong;
    	}
    	else{
    		return System.currentTimeMillis();
    	}
    	
    }
	
    public void saveInitialTime(){
    	PrintWriter pr = null;
    	long now = System.currentTimeMillis();
    	try{
            pr = new PrintWriter(new File(MessageUtil.buildTimeFileName(baseTimeFilePath)));
            pr.println(now);
            logger.debug("@Cesar: Saved timestamp <" + now + ">");
        }
        catch(IOException e){
            logger.error("Exception, cannot save  time", e);
        }
    	finally{
    		if(pr != null) pr.close();
    	}
    }
    
    public void loadInitialTime(){
    	BufferedReader brdr = null;
    	try{
    		brdr = new BufferedReader(new FileReader(new File(MessageUtil.buildTimeFileName(baseTimeFilePath))));
            String time = brdr.readLine();
            baseTimeStamp = Long.valueOf(time);
            logger.debug("@Cesar: Timestamp loaded from timestamp <" + MessageUtil.buildTimeFileName(baseTimeFilePath) + ">");
        }
        catch(IOException e){
            logger.error("Exception, cannot deserialize time", e);
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
    
    public Timer createTimer(){
    	return new Timer();
    }
    
    public void saveElapsedProcessingTime(long elapsed){
    	PrintWriter pr = null;
    	try{
            pr = new PrintWriter(new FileWriter(new File(MessageUtil.buildMessageProcessingTimeMap(baseTimeFilePath)), true));
            pr.println(elapsed);
        }
        catch(IOException e){
            logger.error("Exception, cannot save  time", e);
        }
    	finally{
    		if(pr != null) pr.close();
    	}
    } 
    
    public static class Timer{
	   
	   private long start = 0L;
	   private long end = 0L;
	   private long elapsedNanos = 0L;
	   
	   public void startTimer(){
		   start = System.nanoTime();
	   }
	   
	   public long stopTimer(){
		   end = System.nanoTime();
		   elapsedNanos = end - start;
		   return elapsedNanos;
	   }

	   public long getElapsedNanos() {
		   return elapsedNanos;
	   }
	   
	   
   }
	
}