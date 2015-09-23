package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;
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
    public static final int numStubs = 125;
    public static final int allNodes = numTestNodes + numStubs + 2;

    public static final AtomicInteger idGen = new AtomicInteger(0);
    
    static Map<InetAddress, LinkedList<ForwardedGossip>> propagationModels;
    static Set<InetAddress> startedTestNodes;
    
    static LinkedBlockingQueue<InetAddress[]> gossipQueue;
    
    public static void main(String[] args) throws ConfigurationException, InterruptedException, IOException {
        Random rand = new Random();
        final CassandraProcess seedProcess = new CassandraProcess("/tmp/cass_scale", 1);
        final CassandraProcess observerProcess = new CassandraProcess("/tmp/cass_scale", 2);
        PeerState[] peers = GossipPropagationSim.simulate(allNodes, 3000);
        propagationModels = new HashMap<InetAddress, LinkedList<ForwardedGossip>>();
        startedTestNodes = new HashSet<InetAddress>();
        gossipQueue = new LinkedBlockingQueue<InetAddress[]>();
        // It needs some delay to start seed node
        Thread.sleep(5000);
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
//                System.out.println(address);
//                System.out.println(peers[model]);
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
//        System.out.println(addressList);
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
            stub.sendGossip(seed);
            synchronized (stub) {
                stub.wait();
                System.out.println(stub.getInetAddress() + " finished first gossip with seed");
            }
        }
        heartbeatToSeedThread.start();
        stubGroup.setupTokenState();
        stubGroup.setBootStrappingStatusState();
        stubGroup.setNormalStatusState();
        stubGroup.setSeverityState(0.0);
        stubGroup.setLoad(10000);
        final List<CassandraProcess> testNodeProcesses = new LinkedList<CassandraProcess>();
        for (int i = 0; i < numTestNodes; ++i) {
            testNodeProcesses.add(new CassandraProcess("/tmp/cass_scale", i + 3));
        }
        isTestNodesStarted = true;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            
            @Override
            public void run() {
                System.out.println("Shutting down ... kill all Cassandra");
                seedProcess.terminate();
                observerProcess.terminate();
                for (CassandraProcess testNodeProcess : testNodeProcesses) {
                    testNodeProcess.terminate();
                }
            }
            
        });
        
        Thread gossipForwarder = new Thread(new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        InetAddress[] detail = gossipQueue.take();
                        InetAddress testNode = detail[0];
                        InetAddress startNode = detail[1];
                        LinkedList<ForwardedGossip> forwardedGossip = propagationModels.get(testNode);
                          ForwardedGossip model = null;
                          synchronized (forwardedGossip) {
                              while (model == null || model.forwardHistory().size() == 2) {
                                  model = forwardedGossip.removeFirst();
                              }
                          }
                          long s = System.currentTimeMillis();
                          LinkedList<ForwardEvent> forwardChain = model.forwardHistory();
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
                                  MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN)
                                          .doVerb(msgIn, Integer.toString(idGen.incrementAndGet()));
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
                          long e = System.currentTimeMillis();
                          System.out.println(e - s + " " + e);
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
//        Thread  t = new Thread(new Runnable() {
//
//            @Override
//            public void run() {
//                LinkedList<ForwardedGossip> forwardedGossip = propagationModels.get(testNode);
////                System.out.println(testNode);
////                System.out.println(propagationModels);
//                ForwardedGossip model = null;
//                synchronized (forwardedGossip) {
//                    while (model == null || model.forwardHistory().size() == 2) {
//                        model = forwardedGossip.removeFirst();
//                    }
//                }
//                long s = System.currentTimeMillis();
//                LinkedList<ForwardEvent> forwardChain = model.forwardHistory();
//                ForwardEvent start = forwardChain.removeFirst();
//                ForwardEvent end = forwardChain.removeLast();
//                int previousReceivedTime = start.receivedTime;
//                GossiperStub sendingStub = stubGroup.getStub(startNode);
//                for (ForwardEvent forward : forwardChain) {
//                    int receivedTime = forward.receivedTime;
//                    int waitTime = receivedTime - previousReceivedTime;
//                    try {
//                        Thread.sleep(waitTime * 1000);
//                        GossiperStub receivingStub = stubGroup.getRandomStub();
//                        MessageIn<GossipDigestSyn> msgIn = convertOutToIn(sendingStub.genGossipDigestSyncMsg());
//                        msgIn.setTo(receivingStub.getInetAddress());
//                        MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN)
//                                .doVerb(msgIn, Integer.toString(idGen.incrementAndGet()));
//                        sendingStub = receivingStub;
//                        previousReceivedTime = receivedTime;
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//                int waitTime = end.receivedTime - previousReceivedTime;
//                try {
//                    Thread.sleep(waitTime * 1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                long e = System.currentTimeMillis();
//                System.out.println(e - s + " " + e);
//                sendingStub.sendGossip(observer);
//            }
//            
//        });
//        t.start();
    }
    
    public static <T> MessageIn<T> convertOutToIn(MessageOut<T> msgOut) {
        MessageIn<T> msgIn = MessageIn.create(msgOut.from, msgOut.payload, msgOut.parameters, msgOut.verb, MessagingService.VERSION_12);
        return msgIn;
    }
    
}
