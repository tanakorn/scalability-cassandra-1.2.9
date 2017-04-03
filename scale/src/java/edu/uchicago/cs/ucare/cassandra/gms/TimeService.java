package edu.uchicago.cs.ucare.cassandra.gms;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TimeService {

	private ThreadMXBean threadMxBean = null;
	private long baseTimeStamp = 0L;
	private Map<Long, Long> adjustedTime = new ConcurrentHashMap<Long, Long>(); 
	
	public TimeService(){
		this.threadMxBean = ManagementFactory.getThreadMXBean();
	}
	
	public void setBaseTimeStamp(){
		this.baseTimeStamp = System.currentTimeMillis();
	}
	
	public void adjustThreadTime(long threadId, long time){
		Long oldTime = adjustedTime.get(threadId);
		if(oldTime == null){
			adjustedTime.put(threadId, time);
		}
		else{
			adjustedTime.put(threadId, time + oldTime);
		}
		
	}
	
	public long getAdjustedCurrentTimeMillis(){
		long threadCpuTime = toMillis(threadMxBean.getCurrentThreadCpuTime());
		return baseTimeStamp + threadCpuTime + toMillis((adjustedTime.get(Thread.currentThread().getId()) != null? adjustedTime.get(Thread.currentThread().getId()) : 0L));
	}
	
	public long getCurrentTimeMillis(){
		return System.currentTimeMillis();
	}
	
	public long getCurrentTime(boolean adjustThreadRunningTime){
		if(adjustThreadRunningTime) return getAdjustedCurrentTimeMillis();
		return getCurrentTimeMillis();
	}
	
	private long toMillis(long nanos){
		return nanos / 1000000L;
	}
	
}
