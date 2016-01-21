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
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
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

public class WholeClusterSimulator {

    public static final Set<InetAddress> seeds = new HashSet<InetAddress>();
    public static GossiperStubGroup stubGroup;
    
    public static int numStubs;
    public static final int MAX_NODE = 128;
    public static final int QUARANTINE_DELAY = 10000;

    public static final AtomicInteger idGen = new AtomicInteger(0);
    
    private static Timer timer = new Timer();
    private static Random random = new Random();
    
    private static Logger logger = LoggerFactory.getLogger(ScaleSimulator.class);
    
//    public static LinkedBlockingQueue<MessageIn<GossipDigestSyn>> syncQueue = 
//            new LinkedBlockingQueue<MessageIn<GossipDigestSyn>>();
    
    public static long[] bootGossipExecRecords;
//    public static double[] normalGossipExecRecords;
    
    public static Map<Integer, Map<Integer, Long>> normalGossipExecRecords;
    
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
    public static LinkedBlockingQueue<MessageIn<?>> msgQueue = new LinkedBlockingQueue<MessageIn<?>>();
//    public static Map<InetAddress, LinkedBlockingQueue<MessageIn<?>>> msgQueues = new HashMap<InetAddress, LinkedBlockingQueue<MessageIn<?>>>();
    
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
        if (args.length < 3) {
            System.err.println("Please enter execution_time files");
            System.err.println("usage: WholeClusterSimulator <num_node> <boot_exec> <normal_exec> ");
            System.exit(1);
        }
        numStubs = Integer.parseInt(args[0]);
        bootGossipExecRecords = new long[MAX_NODE];
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
//        String mode = args[4];
//        if (mode.equals("offline")) {
//            buffReader = new BufferedReader(new FileReader(args[2]));
//            while ((line = buffReader.readLine()) != null) {
//                String[] tokens = line.split(" ");
//                int numNormalVersion = Integer.parseInt(tokens[0]);
//                double execTime = Double.parseDouble(tokens[1]) / 1000;
//                normalGossipExecRecords[numNormalVersion] = execTime;
//            }
//            buffReader.close();
//        } else if (mode.equals("real-med")) {
//            buffReader = new BufferedReader(new FileReader(args[3]));
//            SimpleRegression regression = new SimpleRegression();
//            while ((line = buffReader.readLine()) != null) {
//                String[] tokens = line.split(" ");
//                int numNormalVersion = Integer.parseInt(tokens[0]);
//                regression.addData(numNormalVersion, Double.parseDouble(tokens[6]));
//            }
//            buffReader.close();
//            double slope = regression.getSlope();
//            double intercept = regression.getIntercept();
//            for (int i = 0; i < MAX_NODE; ++i) {
//                normalGossipExecRecords[i] = slope * i + intercept;
//                if (normalGossipExecRecords[i] < 0) {
//                    normalGossipExecRecords[i] = 0;
//                }
//            }
//        } else if (mode.equals("real-mean")) {
//            buffReader = new BufferedReader(new FileReader(args[3]));
//            SimpleRegression regression = new SimpleRegression();
//            while ((line = buffReader.readLine()) != null) {
//                String[] tokens = line.split(" ");
//                int numNormalVersion = Integer.parseInt(tokens[0]);
//                regression.addData(numNormalVersion, Double.parseDouble(tokens[2]));
//            }
//            buffReader.close();
//            double slope = regression.getSlope();
//            double intercept = regression.getIntercept();
//            for (int i = 0; i < MAX_NODE; ++i) {
//                normalGossipExecRecords[i] = slope * i + intercept;
//                if (normalGossipExecRecords[i] < 0) {
//                    normalGossipExecRecords[i] = 0;
//                }
//            }
//        } else {
//            System.err.println("Wrong mode");
//            System.exit(1);
//        }
//        Gossiper.registerStatic(StorageService.instance);
//        Gossiper.registerStatic(LoadBroadcaster.instance);
        DatabaseDescriptor.loadYaml();
        GossiperStubGroupBuilder stubGroupBuilder = new GossiperStubGroupBuilder();
        final List<InetAddress> addressList = new LinkedList<InetAddress>();
        for (int i = 1; i <= numStubs; ++i) {
            addressList.add(InetAddress.getByName("127.0.0." + i));
        }
//        for (InetAddress address : addressList) {
//            msgQueues.put(address, new LinkedBlockingQueue<MessageIn<?>>());
//        }
        logger.info("Simulate " + numStubs + " nodes = " + addressList);

        stubGroup = stubGroupBuilder.setClusterId("Test Cluster").setDataCenter("")
                .setNumTokens(1024).setSeeds(seeds).setAddressList(addressList)
                .setPartitioner(new Murmur3Partitioner()).build();
        stubGroup.prepareInitialState();
//        stubGroup.listen();
        // I should start MyGossiperTask here
        timer.schedule(new MyGossiperTask(), 0, 1000);
//        Thread syncProcessThread = new Thread(new syncprocessor());
//        syncprocessthread.start();
//        Thread[] ackProcessThreadPool = new Thread[addressList.size()];
        LinkedList<Thread> ackProcessThreadPool = new LinkedList<Thread>();
        for (InetAddress address : addressList) {
            Thread t = new Thread(new AckProcessor(address));
            ackProcessThreadPool.add(t);
            t.start();
        }
//        for (int i = 0; i < ackProcessThreadPool.length; ++i) {
//           ackProcessThreadPool[i] = new Thread(new AckProcessor());
//           ackProcessThreadPool[i].start();
//        }
        stubGroup.setupTokenState();
        stubGroup.setBootStrappingStatusState();
        for (InetAddress seed : seeds) {
            stubGroup.getStub(seed).setNormalStatusState();
        }
        // Replace hard-coded number here with ring_deplay
        Thread.sleep(10000);
        stubGroup.setNormalStatusState();
        stubGroup.setSeverityState(0.0);
        stubGroup.setLoad(10000);
    }
    
    public static <T> MessageIn<T> convertOutToIn(MessageOut<T> msgOut) {
        MessageIn<T> msgIn = MessageIn.create(msgOut.from, msgOut.payload, msgOut.parameters, msgOut.verb, MessagingService.VERSION_12);
        return msgIn;
    }
    
    static Random rand = new Random();
    public static long getExecTimeNormal(int currentVersion, int numNormal) {
        if (currentVersion > 121) {
            currentVersion = 121;
        }
        if (numNormal > numStubs - currentVersion) {
            numNormal = numStubs - currentVersion;
            numNormal = (numNormal / 4) * 4 + 1;
        }
        if (numNormal > 49) {
            numNormal = 49;
        }
        Long result = normalGossipExecRecords.get(currentVersion).get(numNormal);
        return result;
//        double execTime = normalGossipExecRecords[numNormal];
//        long execTimeMilli = execTime < 0 ? 0 : (long) (execTime * 1000);
//        return execTimeMilli;
//        double sdExecTime = normalGossipExecSdRecords[numNormal];
//        double gaussian = rand.nextGaussian();
//        double adjustedExecTime = execTime + sdExecTime * gaussian;
//        return adjustedExecTime < 0 ? 0 : (long) (adjustedExecTime * 1000);
    }
    
    public static class MyGossiperTask extends TimerTask {

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            for (GossiperStub performer : stubGroup) {
                InetAddress performerAddress = performer.getInetAddress();
                performer.updateHeartBeat();
                boolean gossipToSeed = false;
                Set<InetAddress> liveEndpoints = performer.getLiveEndpoints();
                Set<InetAddress> seeds = performer.getSeeds();
                if (!liveEndpoints.isEmpty()) {
                    InetAddress liveReceiver = GossiperStub.getRandomAddress(liveEndpoints);
                    gossipToSeed = seeds.contains(liveReceiver);
                    MessageIn<GossipDigestSyn> synMsg = performer.genGossipDigestSyncMsgIn(liveReceiver);
                    if (!msgQueue.add(synMsg)) {
                        logger.error("Cannot add more message to message queue");
                    } else {
//                        logger.debug(performerAddress + " sending sync to " + liveReceiver + " " + synMsg.payload.gDigests);
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
                performer.doStatusCheck();
            }
            long finish = System.currentTimeMillis();
            if (finish - start > 1000) {
                logger.warn("It took more than 1 s to do gossip task");
            }
            logger.info("Gossip message in the queue " + msgQueue.size());
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
        
        public AckProcessor(InetAddress address) {
            this.address = address;
        }

        @Override
        public void run() {
//            LinkedBlockingQueue<MessageIn<?>> msgQueue = msgQueues.get(address);
            while (true) {
                try {
                MessageIn<?> ackMessage = msgQueue.take();
//                logger.debug("Processing " + ackMessage.verb + " from " + ackMessage.from + " to " + ackMessage.getTo());
                MessagingService.instance().getVerbHandler(ackMessage.verb).doVerb(ackMessage, Integer.toString(idGen.incrementAndGet()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                if (msgQueue.size() > 1000) {
//                    logger.warn("Ack queue size is greater than 1000");
//                }
            }
        }
        
    }
    
}
