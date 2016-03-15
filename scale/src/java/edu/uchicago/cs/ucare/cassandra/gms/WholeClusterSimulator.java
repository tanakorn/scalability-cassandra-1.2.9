package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.math.stat.regression.SimpleRegression;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uchicago.cs.ucare.scale.ResumeTask;

public class WholeClusterSimulator {

    public static final Set<InetAddress> seeds = new HashSet<InetAddress>();
    public static GossiperStubGroup stubGroup;
    
    public static int numStubs;
    public static final int QUARANTINE_DELAY = 10000;

    public static final AtomicInteger idGen = new AtomicInteger(0);
    
    private static Timer[] timers;
    private static Random random = new Random();
    
    private static Logger logger = LoggerFactory.getLogger(ScaleSimulator.class);
    
    public static long[] bootGossipExecRecords;
    public static Map<Integer, Map<Integer, Long>> normalGossipExecRecords;
    
    public static Map<InetAddress, ConcurrentLinkedQueue<MessageIn<?>>> msgQueues = new HashMap<InetAddress, ConcurrentLinkedQueue<MessageIn<?>>>();
    public static ExecutorService msgProcessors; 
    public static ScheduledExecutorService resumeProcessors;
    public static Map<InetAddress, AtomicBoolean> isProcessing;
    
    public static double[] maxPhiInObserver;
    public static double[] maxPhiOfObservee;

    static Map<Long, Queue<ResumeTask>> taskMap = new ConcurrentHashMap<Long, Queue<ResumeTask>>();
    
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
        if (args.length < 6) {
            System.err.println("Please enter execution_time files");
            System.err.println("usage: WholeClusterSimulator <num_node> <boot_exec> <normal_exec> <num_gossipers> <num_processors> <num_resumer>");
            System.exit(1);
        }
        numStubs = Integer.parseInt(args[0]);
        bootGossipExecRecords = new long[1024];
//        normalGossipExecRecords = new double[MAX_NODE];
        normalGossipExecRecords = new HashMap<Integer, Map<Integer,Long>>();
        maxPhiInObserver = new double[numStubs];
        maxPhiOfObservee = new double[numStubs];
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
//        Gossiper.registerStatic(StorageService.instance);
//        Gossiper.registerStatic(LoadBroadcaster.instance);
        DatabaseDescriptor.loadYaml();
        GossiperStubGroupBuilder stubGroupBuilder = new GossiperStubGroupBuilder();
        final List<InetAddress> addressList = new LinkedList<InetAddress>();
        for (int i = 1; i <= numStubs; ++i) {
            int a = (i - 1) / 255;
            int b = (i - 1) % 255 + 1;
            addressList.add(InetAddress.getByName("127.0." + a + "." + b));
        }
        isProcessing = new HashMap<InetAddress, AtomicBoolean>();
        for (InetAddress address : addressList) {
            msgQueues.put(address, new ConcurrentLinkedQueue<MessageIn<?>>());
            isProcessing.put(address, new AtomicBoolean(false));
        }
        logger.info("Simulate " + numStubs + " nodes = " + addressList);

        stubGroup = stubGroupBuilder.setClusterId("Test Cluster").setDataCenter("")
                .setNumTokens(1024).setSeeds(seeds).setAddressList(addressList)
                .setPartitioner(new Murmur3Partitioner()).build();
        stubGroup.prepareInitialState();
        // I should start MyGossiperTask here
        
//        int numGossiper = numStubs / 100;
//        int numGossiper = 2;
        int numGossiper = Integer.parseInt(args[3]);
        numGossiper = numGossiper == 0 ? 1 : numGossiper;
        timers = new Timer[numGossiper];
        LinkedList<GossiperStub>[] subStub = new LinkedList[numGossiper];
        for (int i = 0; i < numGossiper; ++i) {
            timers[i] = new Timer();
            subStub[i] = new LinkedList<GossiperStub>();
        }
        int ii = 0;
        for (GossiperStub stub : stubGroup) {
            subStub[ii].add(stub);
            ii = (ii + 1) % numGossiper;
        }
        for (int i = 0; i < numGossiper; ++i) {
            timers[i].schedule(new MyGossiperTask(subStub[i]), 0, 1000);
        }
        int numProcessors = Integer.parseInt(args[4]);
        msgProcessors = Executors.newFixedThreadPool(numProcessors);
        int numResumers = Integer.parseInt(args[5]);
        resumeProcessors = Executors.newScheduledThreadPool(numResumers);
        Thread msgProber = new Thread(new MessageProber());
        msgProber.start();
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
    }
    
    public static class MyGossiperTask extends TimerTask {
        
        List<GossiperStub> stubs;
        
        public MyGossiperTask(List<GossiperStub> stubs) {
            this.stubs = stubs;
        }

        @Override
        public void run() {
//            logger.info("Generating gossip syn for " + stubs.size());
            long start = System.currentTimeMillis();
            for (GossiperStub performer : stubs) {
                InetAddress performerAddress = performer.getInetAddress();
                performer.updateHeartBeat();
                boolean gossipToSeed = false;
                Set<InetAddress> liveEndpoints = performer.getLiveEndpoints();
                Set<InetAddress> seeds = performer.getSeeds();
                if (!liveEndpoints.isEmpty()) {
                    InetAddress liveReceiver = GossiperStub.getRandomAddress(liveEndpoints);
                    gossipToSeed = seeds.contains(liveReceiver);
                    MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(liveReceiver);
                    ConcurrentLinkedQueue<MessageIn<?>> msgQueue = msgQueues.get(liveReceiver);
                    if (!msgQueue.add(synMsg)) {
                        logger.error("Cannot add more message to message queue");
                    } else {
//                        logger.debug(performerAddress + " sending sync to " + liveReceiver + " " + synMsg.payload.gDigests);
                    }
                } else {
//                    logger.debug(performerAddress + " does not have live endpoint");
                }
                Map<InetAddress, Long> unreachableEndpoints = performer.getUnreachableEndpoints();
                InetAddress unreachableReceiver = GossiperStub.getRandomAddress(unreachableEndpoints.keySet());
                if (unreachableReceiver != null) {
                    MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(unreachableReceiver);
                    double prob = ((double) unreachableEndpoints.size()) / (liveEndpoints.size() + 1.0);
                    if (prob > random.nextDouble()) {
                        ConcurrentLinkedQueue<MessageIn<?>> msgQueue = msgQueues.get(unreachableReceiver);
                        if (msgQueue == null) {
                            System.out.println("msgQueue is null for " + unreachableReceiver);
                        }
                        if (synMsg == null) {
                            System.out.println("syn msg is null");
                        }
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
                                ConcurrentLinkedQueue<MessageIn<?>> msgQueue = msgQueues.get(seed);
                                if (!msgQueue.add(synMsg)) {
                                    logger.error("Cannot add more message to message queue");
                                } else {
//                                    logger.debug(performerAddress + " sending sync to seed " + seed + " " + synMsg.payload.gDigests);
                                }
                            } else {
                                double probability = seeds.size() / (double)( liveEndpoints.size() + unreachableEndpoints.size() );
                                double randDbl = random.nextDouble();
                                if (randDbl <= probability) {
                                    InetAddress seed = GossiperStub.getRandomAddress(seeds);
                                    MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(seed);
                                    ConcurrentLinkedQueue<MessageIn<?>> msgQueue = msgQueues.get(seed);
                                    if (!msgQueue.add(synMsg)) {
                                        logger.error("Cannot add more message to message queue");
                                    } else {
//                                        logger.debug(performerAddress + " sending sync to seed " + seed + " " + synMsg.payload.gDigests);
                                    }
                                }
                            }
                        }
                    }
                }
//                long dscTime = System.currentTimeMillis();
                performer.doStatusCheck();
//                dscTime = System.currentTimeMillis() - dscTime;
//                System.out.println(dscTime);
            }
            long gossipingTime = System.currentTimeMillis() - start;
            if (gossipingTime > 1000) {
                long lateness = gossipingTime - 1000;
                long totalLateness = lateness * stubs.size();
                logger.warn("Sending lateness " + lateness + " " + totalLateness);
            }
//            logger.info("Gossip message in the queue " + msgQueue.size());
        }
        
    }
    
    public static class MessageProcessor implements Runnable {
        
        MessageIn<?> msg;
        
        public MessageProcessor(MessageIn<?> msg) {
            this.msg = msg;
        }

        @Override
        public void run() {
            MessagingService.instance().getVerbHandler(msg.verb).doVerb(msg, "");
        }
        
    }
    
    public static class MessageProber implements Runnable {

        @Override
        public void run() {
            while (true) {
                boolean isThereMsg = false;
                for (InetAddress address : msgQueues.keySet()) {
                    ConcurrentLinkedQueue<MessageIn<?>> msgQueue = msgQueues.get(address);
//                    logger.info("Checking queue for " + address);
                    if (!msgQueue.isEmpty() && isProcessing.get(address).compareAndSet(false, true)) {
                        MessageIn<?> msg = msgQueue.poll();
                        msgProcessors.execute(new MessageProcessor(msg));
                        isThereMsg = true;
                    } else {
//                        logger.info("There is not a message for " + address + " " + msgQueue.size() + " " + isProcessing.get(address).get());
                    }
                }
//                try {
//                    if (!isThereMsg) {
//                        Thread.sleep(5);
//                    }
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        }
        
    }
    
    public static class RingInfoPrinter implements Runnable {
        
        @Override
        public void run() {
            while (true) {
                boolean isStable = true;
                GossiperStub firstBadNode = null;
                int badNumMemberNode = -1;
                int badNumDeadNode = -1;
                for (GossiperStub stub : stubGroup) {
                    int memberNode = stub.getTokenMetadata().endpointWithTokens.size();
                    int deadNode = 0;
                    for (InetAddress address : stub.endpointStateMap.keySet()) {
                        EndpointState state = stub.endpointStateMap.get(address);
                        if (!state.isAlive()) {
                            deadNode++;
                        }
                    }
                    if (memberNode != numStubs || deadNode > 0) {
                        isStable = false;
                        badNumMemberNode = memberNode;
                        badNumDeadNode = deadNode;
                        firstBadNode = stub;
                        break;
                    }
                }
                int flapping = 0;
                for (GossiperStub stub : stubGroup) {
                    flapping += stub.flapping;
                }
                if (isStable) {
                    logger.info("stable status yes " + flapping + " ; lateness " + ResumeTask.averageLateness() + " " + ResumeTask.maxLateness());
                } else {
                    logger.info("stable status no " + flapping + " ( " 
                            + firstBadNode.getInetAddress() + " " + badNumMemberNode 
                            + " " + badNumDeadNode + " ) ; lateness " + ResumeTask.averageLateness()
                            + " " + ResumeTask.maxLateness());
                }
                StringBuilder sb = new StringBuilder("max_phi_in_observer");
                for (double phi : maxPhiInObserver) {
                    sb.append(phi);
                    sb.append(",");
                }
                logger.info(sb.toString());
                sb = new StringBuilder("max_phi_of_observee");
                for (double phi : maxPhiInObserver) {
                    sb.append(phi);
                    sb.append(",");
                }
                logger.info(sb.toString());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        
        

    }
    
    public static void submitResumeTask(ResumeTask task) {
        long expectedTime = task.getExpectedExecutionTime();
        synchronized (taskMap) {
            if (!taskMap.containsKey(expectedTime)) {
                Queue<ResumeTask> queue = new ConcurrentLinkedQueue<ResumeTask>();
                queue.add(task);
                taskMap.put(expectedTime, queue);
                resumeProcessors.schedule(new BatchResumeTask(expectedTime), expectedTime - System.currentTimeMillis(), TimeUnit.MICROSECONDS);
            } else {
                taskMap.get(expectedTime).add(task);
            }
        }
    }
    
    public static class BatchResumeTask implements Runnable {
        
        long expectedTime;

        public BatchResumeTask(long expectedTime) {
            this.expectedTime = expectedTime;
        }


        @Override
        public void run() {
            Queue<ResumeTask> queue = taskMap.get(expectedTime);
            for (ResumeTask task : queue) {
                task.run();
            }
            synchronized (taskMap) {
                taskMap.remove(expectedTime);
            }
        }
        
    }
    
}
