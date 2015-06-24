package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;

import edu.uchicago.cs.ucare.cassandra.CassandraProcess;

public class ScaleSimulator {

    public static InetAddress seed;

    public static Set<InetAddress> testNodes;
    public static GossiperStubGroup stubGroup;
    
    public static boolean isTestNodesStarted = false;
    
    public static void main(String[] args) throws ConfigurationException, InterruptedException, IOException {
        final CassandraProcess seedProcess = new CassandraProcess("/tmp/cass_scale", 1);
        // It needs some delay to start seed node
        Thread.sleep(5000);
        final int numTestNodes = 9;
        final int numStubs = 60;
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
                int allNodes = numTestNodes + numStubs + 1;
                while (true) {
                    try {
                        Thread.sleep(1000);
                        stubGroup.updateHeartBeat();
                        for (InetAddress address : addressList) {
                            int r = random.nextInt(allNodes);
                            boolean gossipToSeed = false;
                            if (r == 0) {
                                stubGroup.sendOmniscientGossip(seed, address);
                                System.out.println(address + " gossips to seed");
                                gossipToSeed = true;
                            }
                            if (!gossipToSeed || allNodes < 1) {
                                stubGroup.sendOmniscientGossip(seed, address);
                                System.out.println(address + " gossips to seed");
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

}
