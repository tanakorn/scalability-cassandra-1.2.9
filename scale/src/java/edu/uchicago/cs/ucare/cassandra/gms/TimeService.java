package edu.uchicago.cs.ucare.cassandra.gms;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class TimeService {

	private ThreadMXBean threadMxBean = null;
	private long baseTimeStamp = 0L;
	
	public TimeService(){
		this.threadMxBean = ManagementFactory.getThreadMXBean();
	}
	
	public void setBaseTimeStamp(){
		this.baseTimeStamp = System.currentTimeMillis();
	}
	
	public long getAdjustedCurrentTimeMillis(){
		long threadCpuTime = toMillis(threadMxBean.getCurrentThreadCpuTime());
		return baseTimeStamp + threadCpuTime;
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
