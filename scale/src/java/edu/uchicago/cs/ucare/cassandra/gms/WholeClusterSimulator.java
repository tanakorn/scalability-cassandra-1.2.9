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
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.LoadBroadcaster;
import org.apache.cassandra.service.StorageService;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WholeClusterSimulator {

    public static final Set<InetAddress> seeds = new HashSet<InetAddress>();
    public static GossiperStubGroup stubGroup;
    
    public static final int NUM_STUBS = 128;
    public static final int QUARANTINE_DELAY = 10000;

    public static final AtomicInteger idGen = new AtomicInteger(0);
    
    private static Timer timer = new Timer();
    private static Random random = new Random();
    
    private static Logger logger = LoggerFactory.getLogger(ScaleSimulator.class);
    
    public static LinkedBlockingQueue<MessageIn<GossipDigestSyn>> syncQueue = 
            new LinkedBlockingQueue<MessageIn<GossipDigestSyn>>();
    
    public static long[] gossipExecTimeRecords = new long[NUM_STUBS];

    public static PriorityBlockingQueue<MessageOut<?>> ackQueue = new PriorityBlockingQueue<MessageOut<?>>(100, new Comparator<MessageOut<?>>() {

        @Override
        public int compare(MessageOut<?> o1, MessageOut<?> o2) {
            return (int) (o1.getWakeUpTime() - o2.getWakeUpTime());
        }

    });
    
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
            System.out.println(configFileName);
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
        if (args.length < 1) {
            System.err.println("Please enter gossip_exec_time file name");
            System.exit(1);
        }
        BufferedReader buffReader = new BufferedReader(new FileReader(args[0]));
        String line;
        while ((line = buffReader.readLine().trim()) != null) {
            String[] tokens = line.split(" ");
            gossipExecTimeRecords[Integer.parseInt(tokens[0])] = Long.parseLong(tokens[1]);
        }
        buffReader.close();
        Gossiper.registerStatic(StorageService.instance);
        Gossiper.registerStatic(LoadBroadcaster.instance);
        DatabaseDescriptor.loadYaml();
        GossiperStubGroupBuilder stubGroupBuilder = new GossiperStubGroupBuilder();
        final List<InetAddress> addressList = new LinkedList<InetAddress>();
        for (int i = 0; i < NUM_STUBS; ++i) {
            addressList.add(InetAddress.getByName("127.0.0." + i));
        }
        logger.info("Simulate " + NUM_STUBS + " nodes = " + addressList);

        stubGroup = stubGroupBuilder.setClusterId("Test Cluster").setDataCenter("")
                .setNumTokens(1024).setSeeds(seeds).setAddressList(addressList)
                .setPartitioner(new Murmur3Partitioner()).build();
        stubGroup.prepareInitialState();
//        stubGroup.listen();
    }
    
    public static <T> MessageIn<T> convertOutToIn(MessageOut<T> msgOut) {
        MessageIn<T> msgIn = MessageIn.create(msgOut.from, msgOut.payload, msgOut.parameters, msgOut.verb, MessagingService.VERSION_12);
        return msgIn;
    }
    
    public static class MyGossiperTask extends TimerTask {

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            for (GossiperStub stub : stubGroup) {
                boolean gossipToSeed = false;
                Set<InetAddress> liveEndpoints = stub.getLiveEndpoints();
                Set<InetAddress> seeds = stub.getSeeds();
                if (!liveEndpoints.isEmpty()) {
                    InetAddress liveReceiver = GossiperStub.getRandomAddress(liveEndpoints);
                    gossipToSeed = seeds.contains(liveEndpoints);
                    MessageIn<GossipDigestSyn> synMsg = stub.genGossipDigestSyncMsgIn(liveReceiver);
                    if (syncQueue.add(synMsg)) {
                        logger.error("Cannot add more message to message queue");
                    }
                }
                Map<InetAddress, Long> unreachableEndpoints = stub.getUnreachableEndpoints();
                if (!unreachableEndpoints.isEmpty()) {
                    InetAddress unreachableReceiver = GossiperStub.getRandomAddress(unreachableEndpoints.keySet());
                    MessageIn<GossipDigestSyn> synMsg = stub.genGossipDigestSyncMsgIn(unreachableReceiver);
                    double prob = ((double) unreachableEndpoints.size()) / (liveEndpoints.size() + 1.0);
                    if (prob > random.nextDouble()) {
                        if (syncQueue.add(synMsg)) {
                            logger.error("Cannot add more message to message queue");
                        }
                    }
                }
                if (!gossipToSeed || liveEndpoints.size() < seeds.size()) {
                    InetAddress seed = GossiperStub.getRandomAddress(seeds);
                    MessageIn<GossipDigestSyn> synMsg = stub.genGossipDigestSyncMsgIn(seed);
                    if (syncQueue.add(synMsg)) {
                        logger.error("Cannot add more message to message queue");
                    }
                }
                stub.doStatusCheck();
            }
            long finish = System.currentTimeMillis();
            if (finish - start > 1000) {
                logger.warn("It took more than 1 s to do gossip task");
            }
        }
        
    }
    
    public static class SyncProcessor implements Runnable {

        @Override
        public void run() {
           while (true) {
               try {
                MessageIn<GossipDigestSyn> syncMessage = syncQueue.take();
                MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN).doVerb(syncMessage, 
                        Integer.toString(idGen.incrementAndGet()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
           }
        }
        
    }
    
    public static class AckProcessor implements Runnable {

        @Override
        public void run() {
            
        }
        
    }
    
}
