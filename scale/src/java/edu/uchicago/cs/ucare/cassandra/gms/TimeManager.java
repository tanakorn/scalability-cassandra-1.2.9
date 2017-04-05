package edu.uchicago.cs.ucare.cassandra.gms;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;

public class TimeManager {

	public static final TimeManager instance = new TimeManager();
	public static final TimeMeta timeMetaAdjustSendSource = new TimeMeta(TimeMeta.TimeSource.SEND);
	public static final TimeMeta timeMetaAdjustReceiveSource = new TimeMeta(TimeMeta.TimeSource.RECEIVE);
	public static final TimeMeta timeMetaAdjustSendReduce = new TimeMeta(TimeMeta.TimeReduce.GREATER_OF_SEND_RECEIVE);
	public static final TimeMeta timeMetaAdjustReceiveReduce = new TimeMeta(TimeMeta.TimeReduce.GREATER_OF_SEND_RECEIVE);
	public static final TimeMeta timeMetaAdjustMiscReduce = new TimeMeta(TimeMeta.TimeReduce.GREATER_OF_SEND_RECEIVE);
	
	private static Logger logger = LoggerFactory.getLogger(TimeManager.class);
	
	private long baseTimeStamp = 0L;
	private ThreadMXBean threadMxBean = null;
	private boolean timeAdjustEnabled = false;
	private Map<InetAddress, HostTimeManager> timeServicePerHost = new HashMap<InetAddress, HostTimeManager>();
	
	public void initTimeManager(boolean timeAdjustEnabled, Collection<InetAddress> allHosts){
		for(InetAddress host : allHosts){
			timeServicePerHost.put(host, new HostTimeManager(host, threadMxBean));
		}
		baseTimeStamp = System.currentTimeMillis();
		threadMxBean = ManagementFactory.getThreadMXBean();
		this.timeAdjustEnabled = timeAdjustEnabled;
	}
	
	public StringBuilder dumpHostTimeManager(InetAddress host){
		HostTimeManager manager = timeServicePerHost.get(host);
		return manager.dumpHostTimeManager();
	}
	
	public void adjustForHost(InetAddress host, TimeMeta meta){
		HostTimeManager manager = timeServicePerHost.get(host);
		manager.adjustForHost(meta);
	}
	
	public void adjustForHostsWithFixedValue(List<InetAddress> hosts, long fixedValue, TimeMeta meta){
		for(InetAddress host : hosts){
			HostTimeManager manager = timeServicePerHost.get(host);
			manager.adjustForHostWithFixedValue(fixedValue, meta);
		}
	}
	
	public long getCurrentTime(InetAddress host, TimeMeta meta){
		HostTimeManager manager = timeServicePerHost.get(host);
		return manager.getCurrentTime(timeAdjustEnabled, baseTimeStamp, meta);
	}
	
	public long getCurrentTime(List<InetAddress> hosts, TimeMeta meta){
		long maxTime = Long.MIN_VALUE;
		for(InetAddress host : hosts){
			HostTimeManager manager = timeServicePerHost.get(host);
			long time = manager.getCurrentTime(timeAdjustEnabled, baseTimeStamp, meta);
			if(time > maxTime) maxTime = time;
		}
		return maxTime != Long.MIN_VALUE? maxTime : 0L;
	}
	
	private static class HostTimeManager{
		
		private InetAddress hostAddress = null;
		private ThreadMXBean threadMxBean = null;
		private Map<Long, Long> cpuSendTimePerThread = new ConcurrentHashMap<Long, Long>();
		private Map<Long, Long> cpuReceiveTimePerThread = new ConcurrentHashMap<Long, Long>();
		private Map<Long, Long> cpuCommonTimePerThread = new ConcurrentHashMap<Long, Long>();
		private Set<Long> threads = new HashSet<Long>();
		
		public HostTimeManager(InetAddress hostAddress, ThreadMXBean threadMxBean){
			this.hostAddress = hostAddress;
			this.threadMxBean = threadMxBean;
		}
		
		private Map<Long, Long> chooseTimeMap(TimeMeta meta){
			Map<Long, Long> timeMap = 
					meta != null && meta.source != null? 
							(meta.source == TimeMeta.TimeSource.SEND? 
									cpuSendTimePerThread : cpuReceiveTimePerThread) :
							(cpuCommonTimePerThread);
			return timeMap;
		}
		
		private StringBuilder dumpMap(Map<Long, Long> map){
			StringBuilder bld = new StringBuilder();
			for(Entry<Long, Long> entry : map.entrySet()){
				bld.append(String.format("%10d%10d\n", entry.getKey(), toMillis(entry.getValue())));
			}
			return bld;
		}
		
		public StringBuilder dumpHostTimeManager(){
			StringBuilder bld = new StringBuilder();
			bld.append("\ncpuSendTimePerThread\n")
			   .append(String.format("%10s%10s\n", "ThreadId", "Millis"))
			   .append(dumpMap(cpuSendTimePerThread))
			   .append("cpuReceiveTimePerThread\n")
			   .append(String.format("%10s%10s\n", "ThreadId", "Millis"))
			   .append(dumpMap(cpuReceiveTimePerThread));
			return bld;
		}
		
		private long getTimeForHost(TimeMeta meta, Map<Long, Long> map){
			long totalNanos = 0L;
			synchronized(threads){
				for(long threadId : threads){
					Long time = map.get(threadId);
					if(time != null) totalNanos += time;
				}
			}
			return toMillis(totalNanos);
		}
		
		private long toMillis(long value){
			return value / 1000000L;
		}
		
		// apropiate for ack processor threads
		public void adjustForHost(TimeMeta meta){
			try{
				long currentThreadId = Thread.currentThread().getId();
				long threadCpuTime = threadMxBean.getThreadCpuTime(currentThreadId);
				// update the threads per host
				synchronized(threads){
					threads.add(currentThreadId);
				}
				Map<Long, Long> timeMap = chooseTimeMap(meta);
				timeMap.put(currentThreadId, threadCpuTime);
			}
			catch(RuntimeException e){
				logger.error("Error here...", e);
				throw e;
			}
		}
		
		// apropiate for gossiper stubs
		public void adjustForHostWithFixedValue(long fixedNanos, TimeMeta meta){
			long currentThreadId = -1L;
			// update the threads per host
			synchronized(threads){
				threads.add(currentThreadId);
			}
			Map<Long, Long> timeMap = chooseTimeMap(meta);
			// this one adds
			Long time = timeMap.get(currentThreadId);
			if(time != null) fixedNanos += time; 
			timeMap.put(currentThreadId, fixedNanos);
		}
		
		
		public long getCurrentTime(boolean timeAdjustEnabled, long baseTimeStamp, TimeMeta meta){
			if(timeAdjustEnabled) {
				long time = baseTimeStamp;
				if(meta != null && meta.reduce != null){
					if(meta.reduce == TimeMeta.TimeReduce.ALL){
						time += getTimeForHost(meta, cpuSendTimePerThread);
						time += getTimeForHost(meta, cpuReceiveTimePerThread);
					}
					else if(meta.reduce == TimeMeta.TimeReduce.JUST_RECEIVE){
						time += getTimeForHost(meta, cpuReceiveTimePerThread);
					}
					else if(meta.reduce == TimeMeta.TimeReduce.JUST_SEND){
						time += getTimeForHost(meta, cpuSendTimePerThread);
					}
					else if(meta.reduce == TimeMeta.TimeReduce.GREATER_OF_SEND_RECEIVE){
						long sendTime = getTimeForHost(meta, cpuSendTimePerThread);
						long receiveTime = getTimeForHost(meta, cpuReceiveTimePerThread);
						if(sendTime > receiveTime) time += sendTime;
						else time += receiveTime;
					}
					return time;
				}
				else{
					return time + getTimeForHost(meta, cpuCommonTimePerThread);
				}
			}
			else{
				return System.currentTimeMillis();
			}
			
		}
	}
	
	
	public static class TimeMeta{
		
		public static enum TimeSource{
			SEND,
			RECEIVE
		}
		
		public static enum TimeReduce{
			ALL,
			JUST_SEND,
			JUST_RECEIVE,
			GREATER_OF_SEND_RECEIVE
		}
		
		private TimeSource source = null;
		private TimeReduce reduce = null;
		
		public TimeMeta(TimeSource source){
			this.source = source;
		}
		
		public TimeMeta(TimeReduce reduce){
			this.reduce = reduce;
		}
		
	}
	
}
