package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uchicago.cs.ucare.scale.gossip.ForwardedGossip;
import edu.uchicago.cs.ucare.scale.gossip.ForwardedGossip.ForwardEvent;
import edu.uchicago.cs.ucare.scale.gossip.GossipPropagationSim;
import edu.uchicago.cs.ucare.scale.gossip.PeerState;

public class ScaleSimulator {

    public static InetAddress seed;
    public static InetAddress observer;

    public static Set<InetAddress> testNodes;
    public static GossiperStubGroup stubGroup;
    
    public static boolean isTestNodesStarted = false;
    
    public static int numTestNodes;
    public static int numStubs;
    public static int allNodes;

    public static final AtomicInteger idGen = new AtomicInteger(0);
    
    static Map<InetAddress, LinkedList<ForwardedGossip>> propagationModels;
    static Set<InetAddress> startedTestNodes;
    
    static LinkedBlockingQueue<InetAddress[]> gossipQueue;
    
    private static Logger logger = LoggerFactory.getLogger(ScaleSimulator.class);
    
    static
    {
        initLog4j();
    }
    public static void initLog4j()
    {
        if (System.getProperty("log4j.defaultInitOverride","false").equalsIgnoreCase("true"))
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
    }
    
    public static void main(String[] args) throws ConfigurationException, InterruptedException, IOException {
        allNodes = Integer.parseInt(args[0]);
        String id = args[1];
        String[] observerList = args[2].split(",");
        assert observerList.length > 0;
        String[] testNodeList = args[3].split(",");
        assert testNodeList.length > 0;
        numTestNodes = testNodeList.length;
        numStubs = allNodes - 1 - observerList.length - numTestNodes;
        
        Random rand = new Random();
        PeerState[] peers = GossipPropagationSim.simulate(allNodes, 3000);
        propagationModels = new HashMap<InetAddress, LinkedList<ForwardedGossip>>();
        startedTestNodes = new HashSet<InetAddress>();
        gossipQueue = new LinkedBlockingQueue<InetAddress[]>();
        try {
            // Fix seed IP address
            seed = InetAddress.getByName("192.168.1.1");
            observer = InetAddress.getByName(observerList[0]);
            testNodes = new HashSet<InetAddress>();
            for (String testNodeIp : testNodeList) {
                InetAddress address = InetAddress.getByName(testNodeIp);
                testNodes.add(address);
                int model = 0;
                while (model == 0) {
                    model = rand.nextInt(peers.length);
                }
                propagationModels.put(address, peers[model].getModel());
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        DatabaseDescriptor.loadYaml();
        GossiperStubGroupBuilder stubGroupBuilder = new GossiperStubGroupBuilder();
        final List<InetAddress> addressList = new LinkedList<InetAddress>();
        for (int i = 1; i <= numStubs; ++i) {
            addressList.add(InetAddress.getByName("192.168." + id + "." + i));
        }
        logger.info("Testing for " + allNodes + " nodes");
        logger.info("Seed is 192.168.1.1 (fixed)");
        logger.info("Observer is " + observer.toString());
        logger.info("Observed nodes are " + testNodes);
        logger.info("Simulate " + numStubs + " nodes = " + addressList);

        stubGroup = stubGroupBuilder.setClusterId("Test Cluster")
                .setDataCenter("").setNumTokens(1024).setAddressList(addressList)
                .setPartitioner(new Murmur3Partitioner()).build();
        stubGroup.prepareInitialState();
        stubGroup.listen();
        Thread heartbeatToSeedThread = new Thread(new Runnable() {

            @Override
            public void run() {
                Random random = new Random();
                while (true) {
                    try {
                        Thread.sleep(1000);
                        stubGroup.updateHeartBeat();
                        for (InetAddress address : addressList) {
                            int r = random.nextInt(allNodes);
                            boolean gossipToSeed = false;
                            if (r == 0) {
                                GossiperStub stub = stubGroup.getStub(address);
                                stub.sendGossip(seed);
                                gossipToSeed = true;
                            }
                            if (!gossipToSeed || allNodes < 1) {
                                GossiperStub stub = stubGroup.getStub(address);
                                stub.sendGossip(seed);
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            
        });
        for (GossiperStub stub : stubGroup) {
            logger.info("sc_debug: " + stub.getInetAddress() + " send gossip to seed");
            stub.sendGossip(seed);
            synchronized (stub) {
                stub.wait();
                logger.info(stub.getInetAddress() + " finished first gossip with seed with " + stub.getEndpointStateMap().keySet());
            }
        }
        heartbeatToSeedThread.start();
        stubGroup.setupTokenState();
        stubGroup.setBootStrappingStatusState();
        stubGroup.setNormalStatusState();
        stubGroup.setSeverityState(0.0);
        stubGroup.setLoad(10000);
        isTestNodesStarted = true;
        
        Thread gossipForwarder = new Thread(new Runnable() {

            @Override
            @SuppressWarnings("unchecked")
            public void run() {
                while (true) {
                    try {
                        InetAddress[] detail = gossipQueue.take();
                        InetAddress testNode = detail[0];
                        InetAddress startNode = detail[1];
                        LinkedList<ForwardedGossip> forwardedGossip = propagationModels.get(testNode);
//                        logger.info(forwardedGossip.toString());
                        ForwardedGossip model = null;
                        boolean isThereNextModel = false;
                        synchronized (forwardedGossip) {
                            while ((model == null || model.forwardHistory().size() == 2) && !forwardedGossip.isEmpty()) {
                                model = forwardedGossip.removeFirst();
                                isThereNextModel = true;
                            }
                        }
                        if (!isThereNextModel) {
                            logger.info("There is no forwarding model left, generate more");
                            PeerState[] peers = GossipPropagationSim.simulate(allNodes, 1000);
                            Random rand = new Random();
                            int modelIndex = 0;
                            while (modelIndex == 0) {
                                modelIndex = rand.nextInt(peers.length);
                            }
                            propagationModels.put(testNode, peers[modelIndex].getModel());
                            continue;
                        }
                        LinkedList<ForwardEvent> forwardChain = model.forwardHistory();
                        logger.info("Forward chain for " + testNode + " = " + forwardChain);
                        ForwardEvent start = forwardChain.removeFirst();
                        ForwardEvent end = forwardChain.removeLast();
                        int previousReceivedTime = start.receivedTime;
                        GossiperStub sendingStub = stubGroup.getStub(startNode);
                        for (ForwardEvent forward : forwardChain) {
                            int receivedTime = forward.receivedTime;
                            int waitTime = receivedTime - previousReceivedTime;
                            try {
                                Thread.sleep(waitTime * 1000);
                                GossiperStub receivingStub = stubGroup.getRandomStub();
                                MessageIn<GossipDigestSyn> msgIn = convertOutToIn(sendingStub.genGossipDigestSyncMsg());
                                msgIn.setTo(receivingStub.getInetAddress());
                                long s = System.currentTimeMillis();
                                MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN)
                                        .doVerb(msgIn, Integer.toString(idGen.incrementAndGet()));
                                long t = System.currentTimeMillis() - s;
                                logger.info("sc_debug: Doing verb \"" + Verb.GOSSIP_DIGEST_SYN + "\" from " + msgIn.from + " took " + t + " ms");
                                logger.info("sc_debug: Receiving stub is " + receivingStub.getInetAddress() + " with ring " + receivingStub.getEndpointStateMap().keySet());
                                sendingStub = receivingStub;
                                previousReceivedTime = receivedTime;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        int waitTime = end.receivedTime - previousReceivedTime;
                        try {
                            Thread.sleep(waitTime * 1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        logger.info("Forwarding to observer:" + observer + " at " + System.currentTimeMillis());
                        sendingStub.sendGossip(observer);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            
        });
        gossipForwarder.start();

    }
    
    public static void startForwarding(final InetAddress testNode, final InetAddress startNode) {
        InetAddress[] detail = { testNode, startNode };
        gossipQueue.add(detail);
    }
    
    public static <T> MessageIn<T> convertOutToIn(MessageOut<T> msgOut) {
        MessageIn<T> msgIn = MessageIn.create(msgOut.from, msgOut.payload, msgOut.parameters, msgOut.verb, MessagingService.VERSION_12);
        return msgIn;
    }
    
}
