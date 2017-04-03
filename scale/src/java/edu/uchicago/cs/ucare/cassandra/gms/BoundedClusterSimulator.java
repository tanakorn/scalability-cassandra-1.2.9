package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uchicago.cs.ucare.cassandra.gms.WholeClusterSimulator.AckProcessor;

public class BoundedClusterSimulator {

	private static Logger logger = LoggerFactory.getLogger(BoundedClusterSimulator.class);
	
	private static BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<Runnable>();
	private static ExecutorService executorService = null;
	private static Timer gossipTimer = new Timer();
	private static Collection<GossiperStub> stubs = null;
	
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
	    public static long networkQueuedTime = 0;
	    public static int processCount = 0;
		
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
		}
		
	}
	
}
