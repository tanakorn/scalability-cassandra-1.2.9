package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uchicago.cs.ucare.cassandra.gms.GossipMessage.GossipType;

public class WholeClusterSimulator {

    public static final Set<InetAddress> seeds = new HashSet<InetAddress>();
    public static GossiperStubGroup stubGroup;
    
    public static int numStubs;
    public static final int QUARANTINE_DELAY = 10000;

    public static final AtomicInteger idGen = new AtomicInteger(0);
    
    private static Timer[] timers;
    private static MyGossiperTask[] tasks;
    private static Random random = new Random();
    
    private static Logger logger = LoggerFactory.getLogger(ScaleSimulator.class);
    
//    public static LinkedBlockingQueue<MessageIn<GossipDigestSyn>> syncQueue = 
//            new LinkedBlockingQueue<MessageIn<GossipDigestSyn>>();
    
    public static long[] bootGossipExecRecords;
//    public static double[] normalGossipExecRecords;
    
    public static Map<Integer, Map<Integer, Long>> normalGossipExecRecords;
    
    public static long totalProcLateness = 0;
    public static int numProc = 0;
    public static long totalRealSleep = 0;
    public static long totalExpectedSleep = 0;
    public static long maxProcLateness = 0;
    
    public static List<Long> procLatenessList = Collections.synchronizedList(new LinkedList<Long>());
    public static List<Double> percentProcLatenessList = Collections.synchronizedList(new LinkedList<Double>());
    
    public static List<Double> percentSendLatenessList = Collections.synchronizedList(new LinkedList<Double>());
    
    // ##########################################################################
    // @Cesar: some variables
    // ##########################################################################
    public static boolean isSerializationEnabled = Boolean.parseBoolean(System.getProperty("edu.uchicago.ucare.sck.recordSentMessages", "FALSE"));
    public static boolean isReplayEnabled = Boolean.parseBoolean(System.getProperty("edu.uchicago.ucare.sck.replayRecordedMessages", "FALSE"));
    public static boolean failOnStateNotFound = Boolean.parseBoolean(System.getProperty("edu.uchicago.ucare.sck.failOnNotFound", "FALSE"));
    public static String serializationFilePrefix = System.getProperty("edu.uchicago.ucare.sck.serializationFilePrefix", null);
	// ##########################################################################
    // @Cesar: This one handles messages (in record/replay escenario)
    // ##########################################################################
    private static final MessageManager messageManager = new MessageManager(); 
    // ##########################################################################
    
//    public static double[] normalGossipExecSdRecords;
    
//    public static Map<InetAddress, LinkedBlockingQueue<MessageIn<?>>> ackQueues = 
//            new HashMap<InetAddress, LinkedBlockingQueue<MessageIn<?>>>();

//    public static PriorityBlockingQueue<MessageIn<?>> ackQueue = 
//            new PriorityBlockingQueue<MessageIn<?>>(100, new Comparator<MessageIn<?>>() {
//
//        @Override
//        public int compare(MessageIn<?> o1, MessageIn<?> o2) {
//            return (int) (o1.getWakeUpTime() - o2.getWakeUpTime());
//        }
//
//    });
//    public static LinkedBlockingQueue<MessageIn<?>> msgQueue = new LinkedBlockingQueue<MessageIn<?>>();
    public static Map<InetAddress, LinkedBlockingQueue<MessageIn<?>>> msgQueues = new HashMap<InetAddress, LinkedBlockingQueue<MessageIn<?>>>();
    
    public static final Set<InetAddress> observedNodes;
    static {
        observedNodes = new HashSet<InetAddress>();
        String[] tmp = System.getProperty("observed.nodes", "").split(",");
        for (String node : tmp) {
            try {
                observedNodes.add(InetAddress.getByName(node));
            } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                logger.error("Error for when observe {}", node);
            }
        }
    }

    static
    {
        initLog4j();
        try {
            seeds.add(InetAddress.getByName("127.0.0.1"));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    public static void initLog4j()
    {
        String config = System.getProperty("log4j.configuration", "log4j-server.properties");
        URL configLocation = null;
        try
        {
            // try loading from a physical location first.
            configLocation = new URL(config);
        }
        catch (MalformedURLException ex)
        {
            // then try loading from the classpath.
            configLocation = CassandraDaemon.class.getClassLoader().getResource(config);
        }

        if (configLocation == null)
            throw new RuntimeException("Couldn't figure out log4j configuration: "+ config);

        // Now convert URL to a filename
        String configFileName = null;
        try
        {
            // first try URL.getFile() which works for opaque URLs (file:foo) and paths without spaces
            configFileName = configLocation.getFile();
//            System.out.println(configFileName);
            File configFile = new File(configFileName);
            // then try alternative approach which works for all hierarchical URLs with or without spaces
            if (!configFile.exists())
                configFileName = new File(configLocation.toURI()).getCanonicalPath();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Couldn't convert log4j configuration location to a valid file", e);
        }

        PropertyConfigurator.configureAndWatch(configFileName, 10000);
        org.apache.log4j.Logger.getLogger(CassandraDaemon.class).info("Logging initialized");
    }

    public static void main(String[] args) throws ConfigurationException, InterruptedException, IOException {
        if (args.length < 4) {
            System.err.println("Please enter execution_time files");
            System.err.println("usage: WholeClusterSimulator <num_node> <boot_exec> <normal_exec> <num_workers>");
            System.exit(1);
        }
        numStubs = Integer.parseInt(args[0]);
        bootGossipExecRecords = new long[1024];
//        normalGossipExecRecords = new double[MAX_NODE];
        normalGossipExecRecords = new HashMap<Integer, Map<Integer,Long>>();
//        normalGossipExecSdRecords = new double[MAX_NODE];
        System.out.println("Started! " + numStubs);
        BufferedReader buffReader = new BufferedReader(new FileReader(args[1]));
        String line;
        while ((line = buffReader.readLine()) != null) {
            String[] tokens = line.split(" ");
            bootGossipExecRecords[Integer.parseInt(tokens[0])] = Long.parseLong(tokens[1]);
        }
        buffReader.close();
        buffReader = new BufferedReader(new FileReader(args[2]));
        while ((line = buffReader.readLine()) != null) {
            String[] tokens = line.split(" ");
            int currentVersion = Integer.parseInt(tokens[0]);
            int newVersion = Integer.parseInt(tokens[1]);
            long execTime = (long) Double.parseDouble(tokens[2]);
            if (!normalGossipExecRecords.containsKey(currentVersion)) {
                normalGossipExecRecords.put(currentVersion, new HashMap<Integer, Long>());
            }
            normalGossipExecRecords.get(currentVersion).put(newVersion, execTime);
        }
        buffReader.close();
        Gossiper.registerStatic(StorageService.instance);
//        Gossiper.registerStatic(LoadBroadcaster.instance);
        DatabaseDescriptor.loadYaml();
        GossiperStubGroupBuilder stubGroupBuilder = new GossiperStubGroupBuilder();
        final List<InetAddress> addressList = new LinkedList<InetAddress>();
        for (int i = 1; i <= numStubs; ++i) {
            int a = (i - 1) / 255;
            int b = (i - 1) % 255 + 1;
            addressList.add(InetAddress.getByName("127.0." + a + "." + b));
        }
        for (InetAddress address : addressList) {
            msgQueues.put(address, new LinkedBlockingQueue<MessageIn<?>>());
        }
        logger.info("Simulate " + numStubs + " nodes = " + addressList);
        // ##########################################################################
        // @Cesar:Print properties
        // ##########################################################################
        logger.info("@Cesar: isReplayEnabled=" + WholeClusterSimulator.isReplayEnabled + 
        			", isSerializationEnabled=" + WholeClusterSimulator.isSerializationEnabled + 
        			", serializationFilePrefix=" + WholeClusterSimulator.serializationFilePrefix +
        			", failOnStateNotFound=" + WholeClusterSimulator.failOnStateNotFound);
        // ##########################################################################
        // @Cesar: load round manager messages to replay. This are the sent
        // gossiped messages
        // ##########################################################################
        if(WholeClusterSimulator.isReplayEnabled){
        	messageManager.loadSentGossipRounds(WholeClusterSimulator.serializationFilePrefix, addressList);
        	messageManager.loadReceivedMessages(WholeClusterSimulator.serializationFilePrefix, addressList);
        	// also, load time
        	TimePreservingService.loadInitialTime(WholeClusterSimulator.serializationFilePrefix);
        	TimePreservingService.setRelativeTimeStamp();
        }
        else if(WholeClusterSimulator.isSerializationEnabled){
        	TimePreservingService.saveInitialTime(WholeClusterSimulator.serializationFilePrefix);
        }
        // ##########################################################################
        stubGroup = stubGroupBuilder.setClusterId("Test Cluster").setDataCenter("")
                .setNumTokens(32).setSeeds(seeds).setAddressList(addressList)
                .setPartitioner(new Murmur3Partitioner()).build();
        stubGroup.prepareInitialState();
        // I should start MyGossiperTask here
        
//        int numGossiper = numStubs / 100;
//        int numGossiper = 2;
        int numGossiper = Integer.parseInt(args[3]);
        numGossiper = numGossiper == 0 ? 1 : numGossiper;
        timers = new Timer[numGossiper];
        tasks = new MyGossiperTask[numGossiper];
        LinkedList<GossiperStub>[] subStub = new LinkedList[numGossiper];
        for (int i = 0; i < numGossiper; ++i) {
            subStub[i] = new LinkedList<GossiperStub>();
        }
        int ii = 0;
        for (GossiperStub stub : stubGroup) {
            subStub[ii].add(stub);
            ii = (ii + 1) % numGossiper;
        }
        for (int i = 0; i < numGossiper; ++i) {
            timers[i] = new Timer();
            tasks[i] = new MyGossiperTask(subStub[i]);
            timers[i].schedule(tasks[i], 0, 1000);
        }
//        timer.schedule(new MyGossiperTask(), 0, 1000);
        LinkedList<Thread> ackProcessThreadPool = new LinkedList<Thread>();
        for (InetAddress address : addressList) {
            Thread t = new Thread(new AckProcessor(address));
            ackProcessThreadPool.add(t);
            t.start();
        }
        Thread seedThread = new Thread(new Runnable() {
            
            @Override
            public void run() {
                for (InetAddress seed : seeds) {
                    GossiperStub stub = stubGroup.getStub(seed);
                    stub.setupTokenState();
                    stub.updateNormalTokens();
                    stub.setNormalStatusState();
                    stub.setSeverityState(0.0);
                    stub.setLoad(10000);
                }
            }
        });
        seedThread.start();
        
        Thread otherThread = new Thread(new Runnable() {
            
            @Override
            public void run() {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (InetAddress address : addressList) {
                    if (!seeds.contains(address)) {
                        GossiperStub stub = stubGroup.getStub(address);
                        stub.setupTokenState();
                        stub.setBootStrappingStatusState();
                    }
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (InetAddress address : addressList) {
                    if (!seeds.contains(address)) {
                        GossiperStub stub = stubGroup.getStub(address);
                        stub.updateNormalTokens();
                        stub.setupTokenState();
                        stub.setNormalStatusState();
                        stub.setSeverityState(0.0);
                        stub.setLoad(10000);
                    }
                }
            }
        });
        otherThread.start();
        
        Thread infoPrinter = new Thread(new RingInfoPrinter());
        infoPrinter.start();

    }
    
    public static <T> MessageIn<T> convertOutToIn(MessageOut<T> msgOut) {
        MessageIn<T> msgIn = MessageIn.create(msgOut.from, msgOut.payload, msgOut.parameters, msgOut.verb, MessagingService.VERSION_12);
        return msgIn;
    }
    
    static Random rand = new Random();
    public static long getExecTimeNormal(int currentVersion, int numNormal) {
        if (numNormal > numStubs - currentVersion) {
            numNormal = numStubs - currentVersion;
            numNormal = (numNormal / 4) * 4;
        }
        if (numNormal == 0) {
            return 0;
        }
        Long result = normalGossipExecRecords.get(currentVersion).get(numNormal);
        if (result == null) {
            System.out.println(currentVersion + " " + numNormal);
        }
        return result;
//        long execTimeMilli = execTime < 0 ? 0 : (long) (execTime * 1000);
//        return execTimeMilli;
//        double sdExecTime = normalGossipExecSdRecords[numNormal];
//        double gaussian = rand.nextGaussian();
//        double adjustedExecTime = execTime + sdExecTime * gaussian;
//        return adjustedExecTime < 0 ? 0 : (long) (adjustedExecTime * 1000);
    }
    
    public static class MyGossiperTask extends TimerTask {
        
        List<GossiperStub> stubs;
        long previousTime;
        
        long totalIntLength;
        int sentCount;
        
        public MyGossiperTask(List<GossiperStub> stubs) {
            this.stubs = stubs;
            previousTime = 0;
            totalIntLength = 0;
            sentCount = 0;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            if (previousTime != 0) {
                long interval = start - previousTime;
                interval = interval < 1000 ? 1000 : interval;
                totalIntLength += (interval * stubs.size());
                sentCount += stubs.size();
                percentSendLatenessList.add((((double) interval) - 1000.0) / 10.0);
            }
            previousTime = start;
            for (GossiperStub performer : stubs) {
                InetAddress performerAddress = performer.getInetAddress();
                // ##########################################################################
                // @Cesar: Lets replay
                // ##########################################################################
                if(WholeClusterSimulator.isReplayEnabled){
                	GossipRound round = messageManager.pollNextSentMessage(performerAddress);
                	if(round == null){
                		// round for this guy is null, so no more messages to send
                		messageManager.removeSentMessageQueue(performerAddress);
                		// so, do we have any queues left?
                		if(!messageManager.sentMessageQueuesLeft()){
                			// always fail when no more message queues
                			logger.error("@Cesar: Failing cause there are no more messages to gossip in any queue");
                			System.exit(0);
                		}
                	}
                	else{
	                	// update heartbeat
                		performer.updateHeartBeat();
                		// now replay
                		if(logger.isDebugEnabled()) logger.debug("@Cesar: Gossip message to replay, round <" + round.getGossipRound() + ">");
                		if(logger.isDebugEnabled()) logger.debug("@Cesar: To live member: " + (round.getToLiveMember() != null? round.getToLiveMember().getToWho() : null));
                		if(logger.isDebugEnabled()) logger.debug("@Cesar: To unreachable member: " + (round.getToUnreachableMember() != null? round.getToUnreachableMember().getToWho() : null));
                		if(logger.isDebugEnabled()) logger.debug("@Cesar: To seed member: " + (round.getToSeed() != null? round.getToSeed().getToWho() : null));
	                	// get the messages and replay
	                	GossipMessage toLiveMember = round.getToLiveMember();
	                	GossipMessage toUnreachableMember = round.getToUnreachableMember();
	                	GossipMessage toSeed = round.getToSeed();
	                	if(toLiveMember != null){
	                		LinkedBlockingQueue<MessageIn<?>> msgQueue = msgQueues.get(toLiveMember.getToWho());
	                		msgQueue.add(toLiveMember.getMessage());
	                	}
	                	if(toUnreachableMember != null){
	                		LinkedBlockingQueue<MessageIn<?>> msgQueue = msgQueues.get(toUnreachableMember.getToWho());
	                		msgQueue.add(toUnreachableMember.getMessage());
	                	}
	                	if(toSeed != null){
	                		LinkedBlockingQueue<MessageIn<?>> msgQueue = msgQueues.get(toSeed.getToWho());
	                		msgQueue.add(toSeed.getMessage());
	                	}
	                	// do status check
	                	performer.doStatusCheck();
	                	// and get out, but continue in this for loop
	                	logger.debug("@Cesar: Round done for <" + performerAddress + ">");
                	}
	                continue;
                }
                performer.updateHeartBeat();
                boolean gossipToSeed = false;
                Set<InetAddress> liveEndpoints = performer.getLiveEndpoints();
                Set<InetAddress> seeds = performer.getSeeds();
                // ##########################################################################
                // @Cesar: In here, i start saving the round (the possible 3 gossip messages)
                // ##########################################################################
                GossipRound currentRound = new GossipRound(messageManager.getNextRoundFor(performerAddress));
                // ##########################################################################
                if (!liveEndpoints.isEmpty()) {
                    InetAddress liveReceiver = GossiperStub.getRandomAddress(liveEndpoints);
                    gossipToSeed = seeds.contains(liveReceiver);
                    MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(liveReceiver);
                    LinkedBlockingQueue<MessageIn<?>> msgQueue = msgQueues.get(liveReceiver);
                    if (!msgQueue.add(synMsg)) {
                        logger.error("Cannot add more message to message queue");
                    } else {
                    	// ##########################################################################
                        // @Cesar: gossip to live member
                    	// ##########################################################################
                    	if(WholeClusterSimulator.isSerializationEnabled){
	                    	GossipMessage toLiveMember = GossipMessage.build(GossipType.TO_LIVE_MEMBER, synMsg, liveReceiver);
	                    	currentRound.setToLiveMember(toLiveMember);
                    	}
                    	// ##########################################################################
                    }
                    
                } else {
//                    logger.debug(performerAddress + " does not have live endpoint");
                }
                Map<InetAddress, Long> unreachableEndpoints = performer.getUnreachableEndpoints();
                if (!unreachableEndpoints.isEmpty()) {
                    InetAddress unreachableReceiver = GossiperStub.getRandomAddress(unreachableEndpoints.keySet());
                    MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(unreachableReceiver);
                    double prob = ((double) unreachableEndpoints.size()) / (liveEndpoints.size() + 1.0);
                    if (prob > random.nextDouble()) {
                        LinkedBlockingQueue<MessageIn<?>> msgQueue = msgQueues.get(unreachableReceiver);
                        if (!msgQueue.add(synMsg)) {
                            logger.error("Cannot add more message to message queue");
                        } else {
                        	// ##########################################################################
                            // @Cesar: gossip to unreachable member
                        	// ##########################################################################
                        	if(WholeClusterSimulator.isSerializationEnabled){
    	                    	GossipMessage toUnreachableMember = GossipMessage.build(GossipType.TO_UNREACHABLE_MEMBER, synMsg, unreachableReceiver);
    	                    	currentRound.setToUnreachableMember(toUnreachableMember);
                        	}
                        	// ##########################################################################
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
                                LinkedBlockingQueue<MessageIn<?>> msgQueue = msgQueues.get(seed);
                                if (!msgQueue.add(synMsg)) {
                                    logger.error("Cannot add more message to message queue");
                                } else {
//                                  // ##########################################################################
                                    // @Cesar: gossip to seed
                                	// ##########################################################################
                                	if(WholeClusterSimulator.isSerializationEnabled){
            	                    	GossipMessage toSeed = GossipMessage.build(GossipType.TO_SEED, synMsg, seed);
            	                    	currentRound.setToSeed(toSeed);
                                	}
                                	// ##########################################################################
                                }
                            } else {
                                double probability = seeds.size() / (double)( liveEndpoints.size() + unreachableEndpoints.size() );
                                double randDbl = random.nextDouble();
                                if (randDbl <= probability) {
                                    InetAddress seed = GossiperStub.getRandomAddress(seeds);
                                    MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(seed);
                                    LinkedBlockingQueue<MessageIn<?>> msgQueue = msgQueues.get(seed);
                                    if (!msgQueue.add(synMsg)) {
                                        logger.error("Cannot add more message to message queue");
                                    } else {
//                                      // ##########################################################################
                                        // @Cesar: gossip to seed
                                    	// ##########################################################################
                                    	if(WholeClusterSimulator.isSerializationEnabled){
                	                    	GossipMessage toSeed = GossipMessage.build(GossipType.TO_SEED, synMsg, seed);
                	                    	currentRound.setToSeed(toSeed);
                                    	}
                                    	// ##########################################################################
                                    }
                                }
                            }
                        }
                    }
                }
                performer.doStatusCheck();
                // ##########################################################################
                // @Cesar: save the round
            	// ##########################################################################
                if(WholeClusterSimulator.isSerializationEnabled){
                	if(logger.isDebugEnabled()) logger.debug("@Cesar: Recording round: <" + currentRound + ">");
                    messageManager.saveRoundToFile(currentRound, 
                    							   WholeClusterSimulator.serializationFilePrefix, 
                    							   performerAddress);
                }
                // ##########################################################################
            }
//            long gossipingTime = System.currentTimeMillis() - start;
//            if (gossipingTime > 1000) {
//                long lateness = gossipingTime - 1000;
//                long totalLateness = lateness * stubs.size();
//                logger.warn("Sending lateness " + lateness + " " + totalLateness);
//            }
//            logger.info("Gossip message in the queue " + msgQueue.size());
        }
        
    }
    
//    public static class SyncProcessor implements Runnable {
//
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    MessageIn<GossipDigestSyn> syncMessage = syncQueue.take();
////                    logger.debug("Processing " + syncMessage.verb + " from " + syncMessage.from + " to " + syncMessage.getTo());
//                    MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN).doVerb(syncMessage, Integer.toString(idGen.incrementAndGet()));
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                if (syncQueue.size() > 1000) {
//                    logger.warn("Sync queue size is greater than 1000");
//                }
//            }
//        }
//        
//    }
    
    public static class AckProcessor implements Runnable {
        
        InetAddress address;
        
        public static long networkQueuedTime = 0;
        public static int processCount = 0;
        
        public AckProcessor(InetAddress address) {
            this.address = address;
        }

        @Override
        public void run() {
            LinkedBlockingQueue<MessageIn<?>> msgQueue = msgQueues.get(address);
            while (true) {
                try {
	                MessageIn<?> ackMessage = null;
	                // ##########################################################################
	                // @Cesar: in here, we save the received message
	            	// ##########################################################################
	                if(WholeClusterSimulator.isSerializationEnabled){
	                	// how much time did we wait?
	                	long startWaiting = System.currentTimeMillis();
	                	// take from queue
	                	ackMessage = msgQueue.take();
	                	// finish waiting
	                	long endWaiting = System.currentTimeMillis();
	                	// save
	                	ReceivedMessage received = new ReceivedMessage(messageManager.getNextReceivedFor(address));
	                	received.setMessageIn(ackMessage);
	                	received.setWaitForNext(endWaiting - startWaiting);
	                	received.setGeneratedId(idGen.incrementAndGet());
	                	if(logger.isDebugEnabled()) logger.debug("@Cesar: Recording message <"  + received.getMessageRound() + ">");
	                	messageManager.saveMessageToFile(received, 
	                									 WholeClusterSimulator.serializationFilePrefix, 
	                									 address);
	                	// no continue here, we follow normal cycle
	                }
	                // ##########################################################################
	                // @Cesar: in here, we load the received message
	            	// ##########################################################################
	                else if(WholeClusterSimulator.isReplayEnabled){
	                	// reconstruct the message
	                	ReceivedMessage nextMessage = messageManager.pollNextReceivedMessage(address);
	                	if(logger.isDebugEnabled()) logger.debug("@Cesar: Message to replay, round <" + nextMessage.getMessageRound() + ">");
	                	if(logger.isDebugEnabled()) logger.debug("@Cesar: message from: " + (nextMessage.getMessageIn() != null? nextMessage.getMessageIn().from : null));
	                	if(nextMessage == null){
	                		// message for this guy is null, so no more messages to receive
	                		messageManager.removeReceivedMessageQueue(address);
	                		// so, do we have any queues left?
	                		if(!messageManager.receivedMessageQueuesLeft()){
	                			// always fail when no more message queues
	                			logger.error("@Cesar: Failing cause there are no more messages to receive in any queue");
	                			System.exit(0);
	                		}
	                	}
	                	else{
	                		// wait the period for receive
	                		try{
	                			Thread.sleep(nextMessage.getWaitForNext());
	                		}
	                		catch(InterruptedException ie){
	                			logger.error("@Cesar: Interrupted while waiting!");
	                		}
	                		// and now do it
	                		ackMessage = nextMessage.getMessageIn();
		                	// save the time and stuff
		                	long networkQueuedTime = TimePreservingService.getCurrentTimeMillis(WholeClusterSimulator.isReplayEnabled) - ackMessage.createdTime;
		                	AckProcessor.networkQueuedTime += networkQueuedTime;
		                	AckProcessor.processCount += 1;
		                	// process the message
		                	try{
		                		MessagingService.instance().getVerbHandler(ackMessage.verb).doVerb(ackMessage, Integer.toString(nextMessage.getGeneratedId()));
		                	}
		                	catch(Exception e){
		                		logger.error("@Cesar: Unexpected exception while processing message <" + ackMessage + ">", e);
		                		System.exit(0);
		                	}
		                	// continue the cycle
	                	}
	                	continue;
	                }
	                // ##########################################################################
	                long networkQueuedTime = System.currentTimeMillis() - ackMessage.createdTime;
	                AckProcessor.networkQueuedTime += networkQueuedTime;
	                AckProcessor.processCount += 1;
	                MessagingService.instance().getVerbHandler(ackMessage.verb).doVerb(ackMessage, Integer.toString(received.getGeneratedId()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                if (msgQueue.size() > 100) {
//                    logger.info("Backlog for " + address + " " + msgQueue.size());
//                }
            }
        }
        
    }
    
    public static class RingInfoPrinter implements Runnable {
        
        @Override
        public void run() {
            while (true) {
                boolean isStable = true;
                for (GossiperStub stub : stubGroup) {
//                    String thisAddress = stub.getInetAddress().toString();
//                    int seenNode = stub.endpointStateMap.size();
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
                    if (memberNode != numStubs || deadNode > 0) {
                        isStable = false;
                        break;
                    }
                }
                int flapping = 0;
                for (GossiperStub stub : stubGroup) {
                    flapping += stub.flapping;
                }
                long interval = 0;
                int sentCount = 0;
                for (MyGossiperTask task : tasks) {
                    interval += task.totalIntLength;
                    sentCount += task.sentCount;
                }
                interval = sentCount == 0 ? 0 : interval / sentCount;
                long avgProcLateness = numProc == 0 ? 0 : totalProcLateness / numProc;
                double percentLateness = totalExpectedSleep == 0 ? 0 : ((double) totalRealSleep) / (double) totalExpectedSleep;
                percentLateness = percentLateness == 0 ? 0 : (percentLateness - 1) * 100;
//                long avgProcLateness = totalExpectedSleep == 0 ? 0 : totalRealSleep / totalExpectedSleep;
                if (isStable) {
                    logger.info("stable status yes " + flapping +
                            " ; proc lateness " + avgProcLateness + " " + maxProcLateness + " " + percentLateness +
                            " ; send lateness " + interval +
                            " ; network lateness " + (AckProcessor.processCount != 0? AckProcessor.networkQueuedTime / AckProcessor.processCount : 0));
                } else {
                    logger.info("stable status no " + flapping + 
                            " ; proc lateness " + avgProcLateness + " " + maxProcLateness + " " + percentLateness +
                            " ; send lateness " + interval + 
                            " ; network lateness " + (AckProcessor.processCount != 0? AckProcessor.networkQueuedTime / AckProcessor.processCount : 0));
                }
                for (GossiperStub stub : stubGroup) {
                	if(WholeClusterSimulator.isReplayEnabled){
                		int backlog = messageManager.getReceivedMessageQueueSize(stub.getInetAddress());
                		if (backlog > 100) {
	                        logger.info("Backlog of " + stub.getInetAddress() + " " + backlog);
	                    }
                	}
                	else{
	                    LinkedBlockingQueue<MessageIn<?>> queue = msgQueues.get(stub.getInetAddress());
	                    if (queue.size() > 100) {
	                        logger.info("Backlog of " + stub.getInetAddress() + " " + queue.size());
	                    }
                	}
                }
                List<Long> tmpLatenessList = new LinkedList<Long>(procLatenessList);
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
                List<Double> tmpPercentLatenessList = new LinkedList<Double>(percentProcLatenessList);
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
                List<Double> tmpPercentSendLatenessList = new LinkedList<Double>(percentSendLatenessList);
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
