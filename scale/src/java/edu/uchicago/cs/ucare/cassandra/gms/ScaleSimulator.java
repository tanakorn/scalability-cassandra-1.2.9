package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;

public class ScaleSimulator {

    private static ScaleSimulator instance;
    private static InetAddress seed;
    {
        try {
            seed = InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    String clusterId;
    int numNodes;
    int numTokens;
    @SuppressWarnings("rawtypes") IPartitioner partitioner;
    Map<InetAddress, GossiperStub> stubs;

    ScaleSimulator(String clusterId, int numNodes, int numTokens,
            @SuppressWarnings("rawtypes") IPartitioner partitioner)
            throws UnknownHostException {
        this.clusterId = clusterId;
        this.numNodes = numNodes;
        this.numTokens = numTokens;
        this.partitioner = partitioner;
        stubs = new HashMap<InetAddress, GossiperStub>();
        for (int i = 0; i < numNodes; ++i) {
            InetAddress address = InetAddress.getByName("127.0.0." + (i + 2));
            stubs.put(address, new GossiperStub(clusterId, "", address,
                    numTokens, partitioner));
        }
    }

    ScaleSimulator(String clusterId, String ipListFileName, int numTokens,
            @SuppressWarnings("rawtypes") IPartitioner partitioner)
            throws IOException {
        this.clusterId = clusterId;
        this.numTokens = numTokens;
        this.partitioner = partitioner;
        stubs = new HashMap<InetAddress, GossiperStub>();
        BufferedReader reader = new BufferedReader(new FileReader(ipListFileName));
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            InetAddress address = InetAddress.getByName(line);
            stubs.put(address, new GossiperStub(clusterId, "", address,
                    numTokens, partitioner));
        }
        reader.close();
        numNodes = stubs.size();
    }

    public Map<InetAddress, GossiperStub> getStubs() {
        return stubs;
    }

    void prepareInitialState() {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.prepareInitialState();
        }
    }

    void setupTokenState() {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.setupTokenState();
        }
    }

    void setBootStrappingStatusState() {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.setBootStrappingStatusState();
        }
    }

    public void setNormalStatusState() {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.setNormalStatusState();
        }
    }

    void setSeverityState(double severity) {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.setSeverityState(severity);
        }
    }

    void setLoad(double load) {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.setLoad(load);
        }
    }

    void sendGossip(InetAddress node) {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.doGossip(node);
        }
    }

    void updateHeartBeat() {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.updateHeartBeat();
        }
    }

    void listen() throws ConfigurationException {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.listen();
        }
    }

    void gossip() {
        while (true) {
            sendGossip(seed);
            updateHeartBeat();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws ConfigurationException, InterruptedException, IOException {
        DatabaseDescriptor.loadYaml();
        String ipListFileName = "iplist";
        if (args.length > 0) {
            ipListFileName = args[0];
        }
        instance = new ScaleSimulator("Test Cluster", ipListFileName, 1024, new Murmur3Partitioner());
        instance.prepareInitialState();
        instance.listen();
        Thread updateThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    Thread.sleep(5000);
                    instance.setupTokenState();
                    instance.setBootStrappingStatusState();
                    Thread.sleep(5000);
                    instance.setNormalStatusState();
                    instance.setSeverityState(0.0);
                    Thread.sleep(5000);
                    instance.setLoad(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });
        updateThread.start();
        instance.gossip();
    }

    public static ScaleSimulator getInstance() {
        return instance;
    }

}
