package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
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

public class GossipProcessingMetric {

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
        if (args.length < 3) {
            System.err.println("Please specify node status (boot/normal), num diff and repeat run");
            System.exit(1);
        }
        String testStatus = args[0];
        int test = Integer.parseInt(args[1]);
        int repeatRun = Integer.parseInt(args[2]);
        if (testStatus.equals("boot")) {
            
        } else if (testStatus.equals("normal")) {
            
        } else {
            System.err.println("status must be boot/normal");
            System.exit(2);
        }
        Gossiper.registerStatic(StorageService.instance);
        Gossiper.registerStatic(LoadBroadcaster.instance);
        DatabaseDescriptor.loadYaml();
        InetAddress firstNode = InetAddress.getByName("127.0.0.1");
//        for (int i = start; i <= end; ++i) {
//            test(InetAddress.getByName("127.0.0." + i), firstNode, testStatus);
//        }
        String s = (test - 1) + " ";
        for (int i = 0; i < repeatRun; ++i) {
            s += test(InetAddress.getByName("127.0.0." + test), firstNode, testStatus) + " ";
        }
        System.out.println(s);
        System.exit(0);
    }
    
    public static <T> MessageIn<T> convertOutToIn(MessageOut<T> msgOut) {
        MessageIn<T> msgIn = MessageIn.create(msgOut.from, msgOut.payload, msgOut.parameters, msgOut.verb, MessagingService.VERSION_12);
        return msgIn;
    }
    
    public static long test(InetAddress gossiperAddress, InetAddress gossipeeAddress, String testStatus) throws UnknownHostException {
        if (gossiperAddress == null || gossipeeAddress == null) {
            logger.error("Wrong arguments");
            return -1;
        }
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
//        for (GossiperStub stub : stubGroup) {
//            if (prevInitStub != null) {
//                stub.endpointStateMap.putAll(prevInitStub.endpointStateMap);
//            }
//            prevInitStub = stub;
//        }
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
//        System.out.println(gossiperAddress + " " + gossipeeAddress);
        GossiperStub gossiper = stubGroup.getStub(gossiperAddress);
        GossiperStub gossipee = stubGroup.getStub(gossipeeAddress);
        int gossiperSize = gossiper.getEndpointStateMap().size();
        int gossipeeSize = gossipee.getEndpointStateMap().size();
        MessageIn<GossipDigestSyn> msgIn = convertOutToIn(gossiper.genGossipDigestSyncMsg());
        msgIn.setTo(gossipeeAddress);
        long time = System.currentTimeMillis();
        MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN).doVerb(msgIn, Integer.toString(idGen.incrementAndGet()));
        time = System.currentTimeMillis() - time;
//        System.out.println((gossiperSize - gossipeeSize) + " " + time);
        return time;
//        logger.info((gossiperSize - gossipeeSize) + " " + (e - s));
//        try {
//            Process p = Runtime.getRuntime().exec("rm -r /tmp/cassandra/commitlog");
//            p.waitFor();
//            byte[] b = new byte[1024];
//            int r = p.getErrorStream().read(b);
//            if (r >= 0) {
//                System.out.println(p + " " + new String(b, 0, r));
//            }
//            p = Runtime.getRuntime().exec("rm -r /tmp/cassandra/data");
//            p.waitFor();
//            b = new byte[1024];
//            r = p.getErrorStream().read(b);
//            if (r >= 0) {
//                System.out.println(p + " " + new String(b, 0, r));
//            }
//            p = Runtime.getRuntime().exec("rm -r /tmp/cassandra/saved_caches");
//            p.waitFor();
//            b = new byte[1024];
//            r = p.getErrorStream().read(b);
//            if (r >= 0) {
//                System.out.println(p + " " + new String(b, 0, r));
//            }
//        } catch (IOException e1) {
//            // TODO Auto-generated catch block
//            System.out.println(e1.toString());
//            e1.printStackTrace();
//        } catch (InterruptedException e1) {
//            // TODO Auto-generated catch block
//            System.out.println(e1.toString());
//            e1.printStackTrace();
//        }
    }
    
}
