package edu.uchicago.cs.ucare.cassandra.gms;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TimeService {

	private ThreadMXBean threadMxBean = null;
	private long baseTimeStamp = 0L;
	private AtomicLong maxCpuTimeOfAllThreads = new AtomicLong(0);
	
	public TimeService(){
		this.threadMxBean = ManagementFactory.getThreadMXBean();
	}
	
	public void setBaseTimeStamp(){
		this.baseTimeStamp = System.currentTimeMillis();
	}
	
	public void adjustThreadTime(){
		if(maxCpuTimeOfAllThreads.longValue() < threadMxBean.getCurrentThreadCpuTime()) 
			maxCpuTimeOfAllThreads.set(threadMxBean.getCurrentThreadCpuTime());
	}
	
	public long getAdjustedCurrentTimeMillis(){
		return baseTimeStamp + toMillis(maxCpuTimeOfAllThreads.get());
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
