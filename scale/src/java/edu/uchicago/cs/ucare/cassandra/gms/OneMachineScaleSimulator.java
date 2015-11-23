package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
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

import edu.uchicago.cs.ucare.cassandra.CassandraProcess;
import edu.uchicago.cs.ucare.scale.gossip.ForwardedGossip;
import edu.uchicago.cs.ucare.scale.gossip.ForwardedGossip.ForwardEvent;
import edu.uchicago.cs.ucare.scale.gossip.GossipPropagationSim;
import edu.uchicago.cs.ucare.scale.gossip.PeerState;

public class OneMachineScaleSimulator {

    public static InetAddress seed;
    public static InetAddress observer;

    public static Set<InetAddress> testNodes;
    public static GossiperStubGroup stubGroup;
    
    public static boolean isTestNodesStarted = false;
    
    public static final int numTestNodes = 1;
    public static final int numStubs = 61;
    public static final int allNodes = numTestNodes + numStubs + 2;

    public static final AtomicInteger idGen = new AtomicInteger(0);
    
    static Map<InetAddress, LinkedList<ForwardedGossip>> propagationModels;
    
    static LinkedBlockingQueue<InetAddress[]> gossipQueue;
    static Timer timer = new Timer();
    
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
        System.out.println("Start scale check");
        Gossiper.registerStatic(StorageService.instance);
        Gossiper.registerStatic(LoadBroadcaster.instance);
        Random rand = new Random();
        final CassandraProcess seedProcess = new CassandraProcess("/tmp/cass_scale", 1);
        final CassandraProcess observerProcess = new CassandraProcess("/tmp/cass_scale", 2);
        PeerState[] peers = GossipPropagationSim.simulate(allNodes, 3000);
        propagationModels = new HashMap<InetAddress, LinkedList<ForwardedGossip>>();
        gossipQueue = new LinkedBlockingQueue<InetAddress[]>();
        // It needs some delay to start seed node
//        Thread.sleep(5000);
        System.out.println("Initializing scale environment");
        try {
            seed = InetAddress.getByName("127.0.0.1");
            observer = InetAddress.getByName("127.0.0.2");
            testNodes = new HashSet<InetAddress>();
            for (int i = 0; i < numTestNodes; ++i) {
                InetAddress address = InetAddress.getByName("127.0.0." + (i + 3));
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
        for (int i = 0; i < numStubs; ++i) {
            addressList.add(InetAddress.getByName("127.0.0." + (i + numTestNodes + 3)));
        }
        logger.info("Testing for " + allNodes + " nodes");
        logger.info("Seed is 127.0.0.1 (fixed)");
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
                System.out.println("Start heartbeat thread");
//                Random random = new Random();
                GossiperStub clusterStub;
                Set<InetAddress> seeds = new HashSet<InetAddress>();
                seeds.add(seed);
                try {
                    clusterStub = new GossiperStub(InetAddress.getByName("127.0.0.4"), "Test Cluster", "", 1024, seeds, new Murmur3Partitioner());
                } catch (UnknownHostException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                    return;
                }
                while (true) {
                    try {
                        Thread.sleep(1000);
                        stubGroup.updateHeartBeat();
                        clusterStub.updateHeartBeat();
                        for (InetAddress address : addressList) {
                            GossiperStub stub = stubGroup.getStub(address);
                            clusterStub.getEndpointStateMap().put(address, stub.getEndpointState());
                        }
                        clusterStub.sendGossip(seed);
                        for (InetAddress testNode : testNodes) {
                            clusterStub.sendGossip(testNode);
                        }
                        int gen = clusterStub.getEndpointStateMap().get(InetAddress.getByName("127.0.0.4")).getHeartBeatState().getGeneration();
                        int ver = clusterStub.getEndpointStateMap().get(InetAddress.getByName("127.0.0.4")).getHeartBeatState().getHeartBeatVersion();
//                        System.out.println(gen + " " + ver);
//                        System.out.println(clusterStub.heartBeatState.getGeneration() + " " + clusterStub.heartBeatState.getHeartBeatVersion());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (UnknownHostException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
            
        });
        System.out.println("Start first gossip with seed");
        for (GossiperStub stub : stubGroup) {
            logger.info("sc_debug: " + stub.getInetAddress() + " send gossip to seed");
            stub.sendGossip(seed);
            synchronized (stub) {
//                stub.wait();
                logger.info(stub.getInetAddress() + " finished first gossip with seed with " + stub.getEndpointStateMap().keySet());
            }
        }
//        System.out.println("Finish first gossip with seed");
        System.out.println("Initialize gossip state of each node");
        GossiperStub prevInitStub = null;
        for (GossiperStub stub : stubGroup) {
            if (prevInitStub != null) {
                stub.endpointStateMap.putAll(prevInitStub.endpointStateMap);
            }
            logger.info(stub.getInetAddress() + " finished random initialization with " + stub.getEndpointStateMap().size() + " nodes");
            prevInitStub = stub;
        }
//        Collection<GossiperStub> stubList = stubGroup.getAllStubs();
//        GossiperStub[] addressArray = new InetAddress[stubList.size()];
//        addressArray = stubList.toArray(addressArray);
//        InetAddress prevAddress = null;
//        for (InetAddress address : addressArray) {
//            GossiperStub thisStub = stubGroup.getStub(address);
//            if (prevAddress != null) {
//                GossiperStub prevStub = stubGroup.getStub(prevAddress);
//                thisStub.endpointStateMap.putAll(prevStub.endpointStateMap);
//            }
//            prevAddress = address;
////            logger.info(address + " finished random initialization " + thisStub);
//        }
        heartbeatToSeedThread.start();
        stubGroup.setupTokenState();
        stubGroup.setBootStrappingStatusState();
        stubGroup.setNormalStatusState();
        stubGroup.setSeverityState(0.0);
        stubGroup.setLoad(10000);
        final List<CassandraProcess> testNodeProcesses = new LinkedList<CassandraProcess>();
        System.out.println("Start Cassandra test node");
        Thread.sleep(3000);
        for (int i = 0; i < numTestNodes; ++i) {
            testNodeProcesses.add(new CassandraProcess("/tmp/cass_scale", i + 3));
        }
        isTestNodesStarted = true;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            
            @Override
            public void run() {
                seedProcess.terminate();
                observerProcess.terminate();
                for (CassandraProcess testNodeProcess : testNodeProcesses) {
                    testNodeProcess.terminate();
                }
            }
            
        });
        
    }
    
    public static void startForwarding(final InetAddress fromNode, final InetAddress testNode, final InetAddress startNode) {
        ForwardedGossip model = null;
        boolean isThereNextModel = false;
        while (!isThereNextModel) {
            LinkedList<ForwardedGossip> forwardedGossip = propagationModels.get(testNode);
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
        }
        LinkedList<ForwardEvent> forwardChain = model.forwardHistory();
//        System.out.println("Forward chain for " + testNode + " = " + forwardChain);
        IntermediateGossip intGossip = new IntermediateGossip(fromNode, testNode, startNode, forwardChain);
        timer.schedule(new GossipForwardTask(intGossip), intGossip.getDelayAndShift());
    }
    
    public static <T> MessageIn<T> convertOutToIn(MessageOut<T> msgOut) {
        MessageIn<T> msgIn = MessageIn.create(msgOut.from, msgOut.payload, msgOut.parameters, msgOut.verb, MessagingService.VERSION_12);
        return msgIn;
    }
    
    static class IntermediateGossip {
        
        private final InetAddress fromNode;
        private final InetAddress testNode;
        private InetAddress currentNode;
        private final LinkedList<ForwardEvent> forwardChain;
        
        private ForwardEvent prevEvent;
        
        public IntermediateGossip(InetAddress fromNode, InetAddress testNode,
                InetAddress currentNode, LinkedList<ForwardEvent> forwardChain) {
            this.fromNode = fromNode;
            this.testNode = testNode;
            this.currentNode = currentNode;
            this.forwardChain = forwardChain;
            this.forwardChain.removeFirst();
            prevEvent = null;
        }

        public InetAddress getCurrentNode() {
            return currentNode;
        }

        public void setCurrentNode(InetAddress currentNode) {
            this.currentNode = currentNode;
        }

        public InetAddress getFromNode() {
            return fromNode;
        }

        public InetAddress getTestNode() {
            return testNode;
        }

        public LinkedList<ForwardEvent> getForwardChain() {
            return forwardChain;
        }

        public ForwardEvent getPrevEvent() {
            return prevEvent;
        }

        public void setPrevEvent(ForwardEvent prevEvent) {
            this.prevEvent = prevEvent;
        }
        
        public long getDelayAndShift() {
            ForwardEvent nextEvent = forwardChain.removeFirst();
            long delay = prevEvent == null ? 0 : (nextEvent.receivedTime - prevEvent.receivedTime) * 1000;
            prevEvent = nextEvent;
            return delay;
        }
        
    }
    
    static class GossipForwardTask extends TimerTask {
        
        final IntermediateGossip intGossip;

        public GossipForwardTask(IntermediateGossip intGossip) {
            this.intGossip = intGossip;
        }

        @Override
        public void run() {
            Thread t = new Thread(new Runnable() {
                
                @Override
                public void run() {
                    if (intGossip.getForwardChain().size() != 1) {
//                        System.out.println(intGossip.getPrevEvent() + " " + intGossip.getForwardChain());
                        GossiperStub sendingStub = stubGroup.getStub(intGossip.getCurrentNode());
                        GossiperStub receivingStub;
                        do {
                            receivingStub = stubGroup.getRandomStub();
                        } while (sendingStub.equals(receivingStub));
//                        System.out.println(Calendar.getInstance().getTime() + "[" + intGossip + "] from " + sendingStub.getInetAddress() + " to " + receivingStub.getInetAddress());
                        MessageIn<GossipDigestSyn> msgIn = convertOutToIn(sendingStub.genGossipDigestSyncMsg());
                        msgIn.setTo(receivingStub.getInetAddress());
                        MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN)
                                .doVerb(msgIn, Integer.toString(idGen.incrementAndGet()));
                        intGossip.setCurrentNode(receivingStub.getInetAddress());
                        long delay = intGossip.getDelayAndShift();
//                        System.out.println(delay);
                        timer.schedule(new GossipForwardTask(intGossip), delay);
                    } else {
//                        System.out.println(Calendar.getInstance().getTime() + "[" + intGossip + "] from " + intGossip.getCurrentNode() + " to observer");
                        stubGroup.getStub(intGossip.getCurrentNode()).sendGossip(observer);
                    }
                }
            });
            t.start();
        }
        
    }
    
}
