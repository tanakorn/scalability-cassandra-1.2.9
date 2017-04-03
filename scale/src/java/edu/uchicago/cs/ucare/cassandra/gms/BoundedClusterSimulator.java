package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uchicago.cs.ucare.cassandra.gms.WholeClusterSimulator.AckProcessor;
import edu.uchicago.cs.ucare.cassandra.gms.WholeClusterSimulator.MyGossiperTask;

public class BoundedClusterSimulator {

	private static Logger logger = LoggerFactory.getLogger(BoundedClusterSimulator.class);
	
	private static BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<Runnable>();
	private static ExecutorService executorService = null;
	private static Timer gossipTimer = new Timer();
	private static Collection<GossiperStub> stubs = null;
	private static AtomicInteger sentGossipCount = new AtomicInteger(0);
	private static AtomicLong sentInterval = new AtomicLong(0L);
	
	public BoundedClusterSimulator(int nThreads, Collection<GossiperStub> stubs){
		this.executorService = Executors.newFixedThreadPool(nThreads);
		this.stubs = stubs;
	}
	
	public void runCluster(){
		// populate initially
		gossipTimer.scheduleAtFixedRate(new GossiperTimerTask(stubs), 0, 1000);
		// sleep a little
		try{
			Thread.sleep(1000);
			while(tasks.size() > 0){
				Runnable task = tasks.take();
				if(task != null){
					executorService.execute(task);
				}
			}
		}
		catch(InterruptedException ie){
			// nothing here
		}
	}
	
	private static void populateWithGossipTasks(Collection<GossiperStub> stubs){
		for(GossiperStub stub : stubs){
			tasks.add(new GossipTask(stub));
		}
	}
	
	private static void addReceiveTask(InetAddress address){
		tasks.add(new AckProcessorTask(address));
	}
	
	public static class GossiperTimerTask extends TimerTask{

		private Collection<GossiperStub> stubs = null;
		
		public GossiperTimerTask(Collection<GossiperStub> stubs){
			this.stubs = stubs;
		}
		
		@Override
		public void run() {
			populateWithGossipTasks(stubs);
		}
		
	}
	
	public static class AckProcessorTask implements Runnable{

		InetAddress address;
		
	    public AckProcessorTask(InetAddress address) {
            this.address = address;
        }
	    
	    @Override
        public void run() {
            LinkedBlockingQueue<MessageIn<?>> msgQueue = WholeClusterSimulator.msgQueues.get(address);
            while (true) {
                try {
                MessageIn<?> ackMessage = msgQueue.take();
                long networkQueuedTime = System.currentTimeMillis() - ackMessage.createdTime; 
                AckProcessor.networkQueuedTime += networkQueuedTime;
                AckProcessor.processCount += 1;
                MessagingService.instance().getVerbHandler(ackMessage.verb).doVerb(ackMessage, Integer.toString(WholeClusterSimulator.idGen.incrementAndGet()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
		
	}
	
	public static class GossipTask implements Runnable{

		private GossiperStub performer = null;
		
		public GossipTask(GossiperStub performer){
			this.performer = performer;
		}
		
		@Override
		public void run() {
			 InetAddress performerAddress = performer.getInetAddress();
			 performer.updateHeartBeat();
             boolean gossipToSeed = false;
             Set<InetAddress> liveEndpoints = performer.getLiveEndpoints();
             Set<InetAddress> seeds = performer.getSeeds();
             if (!liveEndpoints.isEmpty()) {
                 InetAddress liveReceiver = GossiperStub.getRandomAddress(liveEndpoints);
                 gossipToSeed = seeds.contains(liveReceiver);
                 MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(liveReceiver);
                 LinkedBlockingQueue<MessageIn<?>> msgQueue = WholeClusterSimulator.msgQueues.get(liveReceiver);
                 if (!msgQueue.add(synMsg)) {
                     logger.error("Cannot add more message to message queue");
                 } else {
                	 
                 }
             } else {
            	 
             }
             Map<InetAddress, Long> unreachableEndpoints = performer.getUnreachableEndpoints();
             if (!unreachableEndpoints.isEmpty()) {
                 InetAddress unreachableReceiver = GossiperStub.getRandomAddress(unreachableEndpoints.keySet());
                 MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(unreachableReceiver);
                 double prob = ((double) unreachableEndpoints.size()) / (liveEndpoints.size() + 1.0);
                 if (prob > WholeClusterSimulator.random.nextDouble()) {
                     LinkedBlockingQueue<MessageIn<?>> msgQueue = WholeClusterSimulator.msgQueues.get(unreachableReceiver);
                     if (!msgQueue.add(synMsg)) {
                         logger.error("Cannot add more message to message queue");
                     } else {
                     }
                 }
             }
             if (!gossipToSeed || liveEndpoints.size() < seeds.size()) {
                 int size = seeds.size();
                 if (size > 0) {
                     if (size == 1 && seeds.contains(performerAddress)) {

                     } else {
                         if (liveEndpoints.size() == 0) {
                             InetAddress seed = GossiperStub.getRandomAddress(seeds);
                             MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(seed);
                             LinkedBlockingQueue<MessageIn<?>> msgQueue = WholeClusterSimulator.msgQueues.get(seed);
                             if (!msgQueue.add(synMsg)) {
                                 logger.error("Cannot add more message to message queue");
                             } else {
                            	 
                             }
                         } else {
                             double probability = seeds.size() / (double)( liveEndpoints.size() + unreachableEndpoints.size() );
                             double randDbl = WholeClusterSimulator.random.nextDouble();
                             if (randDbl <= probability) {
                                 InetAddress seed = GossiperStub.getRandomAddress(seeds);
                                 MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(seed);
                                 LinkedBlockingQueue<MessageIn<?>> msgQueue = WholeClusterSimulator.msgQueues.get(seed);
                                 if (!msgQueue.add(synMsg)) {
                                     logger.error("Cannot add more message to message queue");
                                 } else {
                                	 
                                 }
                             }
                         }
                     }
                 }
             }
             performer.doStatusCheck();
             addReceiveTask(performer.getInetAddress());
             sentGossipCount.incrementAndGet();
             sentInterval.set(sentInterval.get() + 1000L);
		}
		
	}
	
	public static class RingInfoPrinter implements Runnable {
        
		private Collection<GossiperStub> stubs = null;
		
		public RingInfoPrinter(Collection<GossiperStub> stubs){
			this.stubs = stubs;
		}
		
        @Override
        public void run() {
            while (true) {
                boolean isStable = true;
                for (GossiperStub stub : stubs) {
                    int memberNode = stub.getTokenMetadata().endpointWithTokens.size();
                    int deadNode = 0;
                    for (InetAddress address : stub.endpointStateMap.keySet()) {
                        EndpointState state = stub.endpointStateMap.get(address);
                        if (!state.isAlive()) {
                            deadNode++;
                        }
                    }
//                    logger.info("ringinfo of " + thisAddress + " seen nodes = " + seenNode + 
//                            ", member nodes = " + memberNode + ", dead nodes = " + deadNode);
                    if (memberNode != stubs.size() || deadNode > 0) {
                        isStable = false;
                        break;
                    }
                }
                int flapping = 0;
                for (GossiperStub stub : stubs) {
                    flapping += stub.flapping;
                }
                long interval = sentInterval.get();
                int sentCount = sentGossipCount.get();
                
                interval = sentCount == 0 ? 0 : interval / sentCount;
                long avgProcLateness = WholeClusterSimulator.numProc == 0 ? 0 : WholeClusterSimulator.totalProcLateness / WholeClusterSimulator.numProc;
                double percentLateness = WholeClusterSimulator.totalExpectedSleep == 0 ? 0 : ((double) WholeClusterSimulator.totalRealSleep) / (double) WholeClusterSimulator.totalExpectedSleep;
                percentLateness = percentLateness == 0 ? 0 : (percentLateness - 1) * 100;
                if (isStable) {
                    logger.info("stable status yes " + flapping +
                            " ; proc lateness " + avgProcLateness + " " + WholeClusterSimulator.maxProcLateness + " " + percentLateness +
                            " ; send lateness " + interval +
                            " ; network lateness " + (AckProcessor.networkQueuedTime / AckProcessor.processCount));
                } else {
                    logger.info("stable status no " + flapping + 
                            " ; proc lateness " + avgProcLateness + " " + WholeClusterSimulator.maxProcLateness + " " + percentLateness +
                            " ; send lateness " + interval + 
                            " ; network lateness " + (AckProcessor.networkQueuedTime / AckProcessor.processCount));
                }
                for (GossiperStub stub : stubs) {
                    LinkedBlockingQueue<MessageIn<?>> queue = WholeClusterSimulator.msgQueues.get(stub.getInetAddress());
                    if (queue.size() > 100) {
                        logger.info("Backlog of " + stub.getInetAddress() + " " + queue.size());
                    }
                }
                List<Long> tmpLatenessList = new LinkedList<Long>(WholeClusterSimulator.procLatenessList);
                TreeMap<Long, Double> latenessDist = new TreeMap<Long, Double>();
                if (tmpLatenessList.size() != 0) {
                    double unit = 1.0 / tmpLatenessList.size();
                    for (Long l : tmpLatenessList) {
                        if (!latenessDist.containsKey(l)) {
                            latenessDist.put(l, 0.0);
                        }
                        latenessDist.put(l, latenessDist.get(l) + unit);
                    }
                    StringBuilder sb = new StringBuilder();
                    double totalCdf = 0.0;
                    for (Long l : latenessDist.keySet()) {
                        double dist = latenessDist.get(l);
                        sb.append(l);
                        sb.append("=");
                        sb.append(totalCdf + dist);
                        sb.append(",");
                        totalCdf += dist;
                    }
                    logger.info("abs_lateness " + sb.toString());
                }
                List<Double> tmpPercentLatenessList = new LinkedList<Double>(WholeClusterSimulator.percentProcLatenessList);
                TreeMap<Double, Double> percentLatenessDist = new TreeMap<Double, Double>();
                if (tmpPercentLatenessList.size() != 0) {
                    double unit = 1.0 / tmpPercentLatenessList.size();
                    for (Double d : tmpPercentLatenessList) {
                        Double roundedD = (double) Math.round(d * 100.0) / 100.0;
                        if (!percentLatenessDist.containsKey(roundedD)) {
                            percentLatenessDist.put(roundedD, 0.0);
                        }
                        percentLatenessDist.put(roundedD, percentLatenessDist.get(roundedD) + unit);
                    }
                    StringBuilder sb = new StringBuilder();
                    double totalCdf = 0.0;
                    for (Double d : percentLatenessDist.keySet()) {
                        double dist = percentLatenessDist.get(d);
                        sb.append(d);
                        sb.append("=");
                        sb.append(totalCdf + dist);
                        sb.append(",");
                        totalCdf += dist;
                    }
                    logger.info("perc_lateness " + sb.toString());
                }
                List<Double> tmpPercentSendLatenessList = new LinkedList<Double>(WholeClusterSimulator.percentSendLatenessList);
                TreeMap<Double, Double> percentSendLatenessDist = new TreeMap<Double, Double>();
                if (tmpPercentSendLatenessList.size() != 0) {
                    double unit = 1.0 / tmpPercentSendLatenessList.size();
                    for (Double d : tmpPercentSendLatenessList) {
                        Double roundedD = (double) Math.round(d * 100.0) / 100.0;
                        if (!percentSendLatenessDist.containsKey(roundedD)) {
                            percentSendLatenessDist.put(roundedD, 0.0);
                        }
                        percentSendLatenessDist.put(roundedD, percentSendLatenessDist.get(roundedD) + unit);
                    }
                    StringBuilder sb = new StringBuilder();
                    double totalCdf = 0.0;
                    for (Double d : percentSendLatenessDist.keySet()) {
                        double dist = percentSendLatenessDist.get(d);
                        sb.append(d);
                        sb.append("=");
                        sb.append(totalCdf + dist);
                        sb.append(",");
                        totalCdf += dist;
                    }
                    logger.info("perc_send_lateness " + sb.toString());
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
	}
	
}