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
	private AtomicLong averageCpuTimeOfAllThreads = new AtomicLong(0);
	private AtomicLong numMetrics = new AtomicLong(0);
	
	public TimeService(){
		this.threadMxBean = ManagementFactory.getThreadMXBean();
	}
	
	public void setBaseTimeStamp(){
		this.baseTimeStamp = System.currentTimeMillis();
	}
	
	public void adjustThreadTime(){
		long oldMt = numMetrics.incrementAndGet() - 1;
		averageCpuTimeOfAllThreads.set(
				(averageCpuTimeOfAllThreads.get() / (oldMt > 0? oldMt : 1L) + 
				threadMxBean.getCurrentThreadCpuTime()) / numMetrics.get());
	}
	
	public long getAdjustedCurrentTimeMillis(){
		return baseTimeStamp + toMillis(averageCpuTimeOfAllThreads.get());
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
