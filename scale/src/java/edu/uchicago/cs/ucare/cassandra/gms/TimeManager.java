package edu.uchicago.cs.ucare.cassandra.gms;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ArrayListMultimap;

public class TimeManager {

	public static final TimeManager instance = new TimeManager();
	public static final TimeMeta timeMetaAdjustSendSource = new TimeMeta(TimeMeta.TimeSource.SEND);
	public static final TimeMeta timeMetaAdjustReceiveSource = new TimeMeta(TimeMeta.TimeSource.RECEIVE);
	public static final TimeMeta timeMetaAdjustSendReduce = new TimeMeta(TimeMeta.TimeReduce.GREATER_OF_SEND_RECEIVE);
	public static final TimeMeta timeMetaAdjustReceiveReduce = new TimeMeta(TimeMeta.TimeReduce.GREATER_OF_SEND_RECEIVE);
	public static final TimeMeta timeMetaAdjustMiscReduce = new TimeMeta(TimeMeta.TimeReduce.GREATER_OF_SEND_RECEIVE);
	
	
	private ArrayListMultimap<InetAddress, Long> threadsPerHost = ArrayListMultimap.create();
	private Map<Long, Long> cpuSendTimePerThread = new ConcurrentHashMap<Long, Long>();
	private Map<Long, Long> cpuReceiveTimePerThread = new ConcurrentHashMap<Long, Long>();
	private Map<Long, Long> cpuCommonTimePerThread = new ConcurrentHashMap<Long, Long>();
	private long baseTimeStamp = 0L;
	private ThreadMXBean threadMxBean = null;
	private boolean timeAdjustEnabled = false;
	
	
	public void initTimeManager(boolean timeAdjustEnabled){
		baseTimeStamp = System.currentTimeMillis();
		threadMxBean = ManagementFactory.getThreadMXBean();
		this.timeAdjustEnabled = timeAdjustEnabled;
	}
	
	public void adjustForHost(InetAddress host, TimeMeta meta){
		long currentThreadId = Thread.currentThread().getId();
		long threadCpuTime = threadMxBean.getThreadCpuTime(currentThreadId);
		// update the threads per host
		synchronized(threadsPerHost){
			threadsPerHost.put(host, currentThreadId);
		}
		// now check
		for(long threadId : threadsPerHost.get(host)){
			if(threadId == currentThreadId){
				Map<Long, Long> timeMap = chooseTimeMap(meta);
				Long time = timeMap.get(threadId);
				if((time != null && time <= threadCpuTime) || (time == null)){
					timeMap.put(threadId, threadCpuTime);
				}
				break;
			}
		}
	}
	
	private Map<Long, Long> chooseTimeMap(TimeMeta meta){
		Map<Long, Long> timeMap = 
				meta != null && meta.source != null? 
						(meta.source == TimeMeta.TimeSource.SEND? 
								cpuSendTimePerThread : cpuReceiveTimePerThread) :
						cpuCommonTimePerThread;
		return timeMap;
	}
	
	private long getTimeForHost(InetAddress host, TimeMeta meta, Map<Long, Long> map){
		long totalNanos = 0L;
		for(long threadId : threadsPerHost.get(host)){
			Long time = map.get(threadId);
			if(time != null) totalNanos += time;
		}
		return toMillis(totalNanos);
	}
	
	public long getCurrentTime(InetAddress host, TimeMeta meta){
		if(timeAdjustEnabled) {
			long time = baseTimeStamp;
			if(meta != null && meta.reduce != null){
				if(meta.reduce == TimeMeta.TimeReduce.ALL){
					time += getTimeForHost(host, meta, cpuSendTimePerThread);
					time += getTimeForHost(host, meta, cpuReceiveTimePerThread);
				}
				else if(meta.reduce == TimeMeta.TimeReduce.JUST_RECEIVE){
					time += getTimeForHost(host, meta, cpuReceiveTimePerThread);
				}
				else if(meta.reduce == TimeMeta.TimeReduce.JUST_SEND){
					time += getTimeForHost(host, meta, cpuSendTimePerThread);
				}
				else if(meta.reduce == TimeMeta.TimeReduce.GREATER_OF_SEND_RECEIVE){
					long sendTime = getTimeForHost(host, meta, cpuSendTimePerThread);
					long receiveTime = getTimeForHost(host, meta, cpuReceiveTimePerThread);
					if(sendTime > receiveTime) time += sendTime;
					else time += receiveTime;
				}
				return time;
			}
			else{
				return time + getTimeForHost(host, meta, cpuCommonTimePerThread);
			}
		}
		else{
			return System.currentTimeMillis();
		}
		
	}
	
	private long toMillis(long value){
		return value / 1000000L;
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
