package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;

import edu.uchicago.cs.ucare.cassandra.CassandraProcess;
import edu.uchicago.cs.ucare.cassandra.gms.GossiperStubGroup.OmniscientGossiperStub.ClockEndpointState;

public class ScaleSimulator {

    public static InetAddress seed;

    public static Set<InetAddress> testNodes;
    public static GossiperStubGroup stubGroup;
    
    public static boolean isTestNodesStarted = false;
    
    public static final int numTestNodes = 9;
    public static final int numStubs = 90;
    public static final int allNodes = numTestNodes + numStubs + 1;
    
    public static void main(String[] args) throws ConfigurationException, InterruptedException, IOException {
        final CassandraProcess seedProcess = new CassandraProcess("/tmp/cass_scale", 1);
        // It needs some delay to start seed node
        Thread.sleep(5000);
        try {
            seed = InetAddress.getByName("127.0.0.1");
            testNodes = new HashSet<InetAddress>();
            for (int i = 0; i < numTestNodes; ++i) {
                testNodes.add(InetAddress.getByName("127.0.0." + (i + 2)));
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        DatabaseDescriptor.loadYaml();
        GossiperStubGroupBuilder stubGroupBuilder = new GossiperStubGroupBuilder();
        final List<InetAddress> addressList = new LinkedList<InetAddress>();
        for (int i = 0; i < numStubs; ++i) {
            addressList.add(InetAddress.getByName("127.0.0." + (i + numTestNodes + 2)));
        }
        System.out.println(addressList);
        stubGroup = stubGroupBuilder.setClusterId("Test Cluster")
                .setDataCenter("").setNumTokens(1024).setAddressList(addressList)
                .setPartitioner(new Murmur3Partitioner()).build();
        stubGroup.prepareInitialState();
        stubGroup.listen();
        Thread heartbeatThread = new Thread(new Runnable() {

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
                                stubGroup.sendOmniscientGossip(seed, address);
//                                System.out.println(address + " gossips to seed");
                                gossipToSeed = true;
                            }
                            if (!gossipToSeed || allNodes < 1) {
                                stubGroup.sendOmniscientGossip(seed, address);
//                                System.out.println(address + " gossips to seed");
                            }
                        }
                        if (isTestNodesStarted) {
                            Map<InetAddress, TreeMap<Integer, ClockEndpointState>> testNodeStatesByVersion = 
                                    stubGroup.getOmniscientGossiperStub().testNodeStatesByVersion;
                            for (InetAddress forwardedNode : testNodeStatesByVersion.keySet()) {
                                TreeMap<Integer, ClockEndpointState> clockEndpointStateByVersion = testNodeStatesByVersion.get(forwardedNode);
                                for (InetAddress testNode : testNodes) {
                                    for (Integer version : clockEndpointStateByVersion.descendingKeySet()) {
                                        if (version == -1) {
                                            continue;
                                        }
                                        ClockEndpointState clockEpState = clockEndpointStateByVersion.get(version);
                                        double currentNumInfection = clockEpState.getNumInfection();
                                        double infectionProb = infectionProbability(allNodes, currentNumInfection);
                                        double r = random.nextDouble();
                                        if (r < infectionProb) {
                                            InetAddress onBehalfOf = addressList.get(random.nextInt(addressList.size()));
                                            GossiperStub sendingStub = stubGroup.getStub(onBehalfOf);
                                            if (sendingStub.setEndpointStateIfNewer(testNode, clockEpState.endpointState)) {
                                                sendingStub.sendGossip(testNode);
                                                break;
                                            }
                                        }
                                    }
                                }
                                for (Integer newVersion : clockEndpointStateByVersion.keySet()) {
                                    ClockEndpointState newState = clockEndpointStateByVersion.get(newVersion);
                                    double newInfection = newState.getNumInfection();
                                    double newInfectionProb = infectionProbability(allNodes, newInfection);
                                    for (Integer oldVersion : clockEndpointStateByVersion.headMap(newVersion).keySet()) {
                                        ClockEndpointState oldState = clockEndpointStateByVersion.get(oldVersion);
                                        double oldInfection = oldState.getNumInfection();
                                        double infectionToOld = (oldInfection * newInfectionProb);
                                        if (infectionToOld > oldInfection) {
                                            newInfection += oldInfection;
                                            oldInfection = 0.0;
                                        } else {
                                            newInfection += infectionToOld;
                                            oldInfection -= infectionToOld;
                                        }
                                        oldState.setNumInfection(oldInfection);
                                    }
                                    newState.setNumInfection(newInfection);
                                }
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
        heartbeatThread.start();
        stubGroup.setupTokenState();
        stubGroup.setBootStrappingStatusState();
        stubGroup.setNormalStatusState();
        stubGroup.setSeverityState(0.0);
        stubGroup.setLoad(10000);
        final List<CassandraProcess> testNodeProcesses = new LinkedList<CassandraProcess>();
        for (int i = 2; i < 2 + numTestNodes; ++i) {
            testNodeProcesses.add(new CassandraProcess("/tmp/cass_scale", i));
        }
        isTestNodesStarted = true;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            
            @Override
            public void run() {
                System.out.println("Shutting down ... kill all Cassandra");
                seedProcess.terminate();
                for (CassandraProcess testNodeProcess : testNodeProcesses) {
                    testNodeProcess.terminate();
                }
            }
            
        });
        
        

//        Thread updateThread = new Thread(new Runnable() {
//
//            @Override
//            public void run() {
//                try {
//                    Thread.sleep(5000);
//                    stubGroup.setupTokenState();
//                    stubGroup.setBootStrappingStatusState();
//                    Thread.sleep(5000);
//                    stubGroup.setNormalStatusState();
//                    stubGroup.setSeverityState(0.0);
//                    Thread.sleep(5000);
//                    stubGroup.setLoad(10000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//
//        });
//        updateThread.start();
//        while (true) {
//            stubGroup.sendGossip(seed);
//            stubGroup.updateHeartBeat();
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
    }
    
    public static double infectionProbability(double allNodes, double currentNumInfection) {
        return 1 - Math.pow((1.0 - 1.0 / (allNodes - 1)), currentNumInfection);
    }
    
    public static double nextNumInfection(double allNodes, double currentNumInfection) {
        return currentNumInfection + (allNodes - currentNumInfection) * infectionProbability(allNodes, currentNumInfection);
    }
    
    public static double infectionIncrease(double allNodes, double currentNumInfection) {
        return (allNodes - currentNumInfection) * infectionProbability(allNodes, currentNumInfection);
    }

}
