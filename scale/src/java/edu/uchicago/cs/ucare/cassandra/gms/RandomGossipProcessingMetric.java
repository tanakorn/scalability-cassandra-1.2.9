package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
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

public class RandomGossipProcessingMetric {

    public static GossiperStubGroup stubGroup;
    
    public static final int numStubs = 128;

    public static final AtomicInteger idGen = new AtomicInteger(0);
    
    private static Logger logger = LoggerFactory.getLogger(ScaleSimulator.class);
    
    static
    {
        initLog4j();
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
        if (args.length < 2) {
            System.err.println("Please specify node status (boot/normal) and the number of new version");
            System.exit(1);
        }
        String testStatus = args[0];
        if (testStatus.equals("boot")) {
            
        } else if (testStatus.equals("normal")) {
            
        } else {
            System.err.println("status must be boot/normal");
            System.exit(2);
        }
        int numNewVersion = Integer.parseInt(args[1]);
        Gossiper.registerStatic(StorageService.instance);
        Gossiper.registerStatic(LoadBroadcaster.instance);
        DatabaseDescriptor.loadYaml();
        InetAddress firstNode = InetAddress.getByName("127.0.0.1");
        for (int i = 1; i < numStubs; ++i) {
            randomTest(i, testStatus);
        }
        System.exit(0);
    }
    
    public static <T> MessageIn<T> convertOutToIn(MessageOut<T> msgOut) {
        MessageIn<T> msgIn = MessageIn.create(msgOut.from, msgOut.payload, msgOut.parameters, msgOut.verb, MessagingService.VERSION_12);
        return msgIn;
    }
    
    public static void randomTest(int newVersion, String testStatus) throws UnknownHostException {
        assert newVersion < numStubs;
        Random rand = new Random();

        GossiperStubGroupBuilder stubGroupBuilder = new GossiperStubGroupBuilder();
        final List<InetAddress> addressList = new LinkedList<InetAddress>();
        for (int i = 1; i <= numStubs; ++i) {
            addressList.add(InetAddress.getByName("127.0.0." + i));
        }
        logger.info("Simulate " + numStubs + " nodes = " + addressList);

        stubGroup = stubGroupBuilder.setClusterId("Test Cluster")
                .setDataCenter("").setNumTokens(1024).setAddressList(addressList)
                .setPartitioner(new Murmur3Partitioner()).build();
        stubGroup.prepareInitialState();
        stubGroup.setupTokenState();
        if (testStatus.equals("boot")) {
            stubGroup.setBootStrappingStatusState();
        } else if (testStatus.equals("normal")) {
            stubGroup.setNormalStatusState();
        } else {
            assert false;
        }

        int gossipeeSize = rand.nextInt(numStubs - newVersion) + 1;
//        GossiperStub gossipee = new GossiperStub(InetAddress.getByName("127.0.0.1"), "Test Cluster", "", 1024, new Murmur3Partitioner());
        InetAddress gossipeeAddress = InetAddress.getByName("127.0.0.1");
        GossiperStub gossipee = stubGroup.getStub(gossipeeAddress);
        while (gossipee.endpointStateMap.size() != gossipeeSize) {
            int index = rand.nextInt(numStubs) + 1;
            InetAddress address = InetAddress.getByName("127.0.0." + index);
            if (index == 2 || gossipee.endpointStateMap.containsKey(address)) {
                continue;
            }
            GossiperStub stub = stubGroup.getStub(address);
            gossipee.endpointStateMap.putAll(stub.endpointStateMap);
        }

        InetAddress gossiperAddress = InetAddress.getByName("127.0.0.2");
//        GossiperStub gossiper = new GossiperStub(InetAddress.getByName("127.0.0.2"), "Test Cluster", "", 1024, new Murmur3Partitioner());
        GossiperStub gossiper = stubGroup.getStub(gossiperAddress);
        while (gossiper.endpointStateMap.size() != newVersion) {
            int index = rand.nextInt(numStubs) + 1;
            InetAddress address = InetAddress.getByName("127.0.0." + index);
            if (gossiper.endpointStateMap.containsKey(address) || gossipee.endpointStateMap.containsKey(address)) {
                continue;
            }
            GossiperStub stub = stubGroup.getStub(address);
            gossiper.endpointStateMap.putAll(stub.endpointStateMap);
        }

        MessageIn<GossipDigestSyn> msgIn = convertOutToIn(gossiper.genGossipDigestSyncMsg());
        msgIn.setTo(gossipeeAddress);
        long s = System.currentTimeMillis();
        MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN).doVerb(msgIn, Integer.toString(idGen.incrementAndGet()));
        long e = System.currentTimeMillis();
        System.out.println(newVersion + " " + (e - s));
    }
    
    public static void test(InetAddress gossiperAddress, InetAddress gossipeeAddress, String testStatus) throws UnknownHostException {
        GossiperStubGroupBuilder stubGroupBuilder = new GossiperStubGroupBuilder();
        final List<InetAddress> addressList = new LinkedList<InetAddress>();
        for (int i = 1; i <= numStubs; ++i) {
            addressList.add(InetAddress.getByName("127.0.0." + i));
        }
        logger.info("Simulate " + numStubs + " nodes = " + addressList);

        stubGroup = stubGroupBuilder.setClusterId("Test Cluster")
                .setDataCenter("").setNumTokens(1024).setAddressList(addressList)
                .setPartitioner(new Murmur3Partitioner()).build();
        stubGroup.prepareInitialState();
        GossiperStub prevInitStub = null;
        for (int i = 1; i <= numStubs; ++i) {
            GossiperStub stub = stubGroup.getStub(InetAddress.getByName("127.0.0." + i));
            if (prevInitStub != null) {
                stub.endpointStateMap.putAll(prevInitStub.endpointStateMap);
            }
            prevInitStub = stub;
        }
        stubGroup.setupTokenState();
        if (testStatus.equals("boot")) {
            stubGroup.setBootStrappingStatusState();
        } else if (testStatus.equals("normal")) {
            stubGroup.setNormalStatusState();
        } else {
            assert false;
        }
        stubGroup.setSeverityState(0.0);
        stubGroup.setLoad(10000);
        GossiperStub gossiper = stubGroup.getStub(gossiperAddress);
        GossiperStub gossipee = stubGroup.getStub(gossipeeAddress);
        int gossiperSize = gossiper.getEndpointStateMap().size();
        int gossipeeSize = gossipee.getEndpointStateMap().size();
        MessageIn<GossipDigestSyn> msgIn = convertOutToIn(gossiper.genGossipDigestSyncMsg());
        msgIn.setTo(gossipeeAddress);
        long s = System.currentTimeMillis();
        MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN).doVerb(msgIn, Integer.toString(idGen.incrementAndGet()));
        long e = System.currentTimeMillis();
        System.out.println((gossiperSize - gossipeeSize) + " " + (e - s));
    }
    
}
