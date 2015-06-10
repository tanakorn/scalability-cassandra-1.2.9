package edu.uchicago.cs.ucare.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;

public class ScaleSimulator {

    private static InetAddress seed;

    public static GossiperStubGroup stubGroup;

    public static void main(String[] args) throws ConfigurationException, InterruptedException, IOException {
        try {
            seed = InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        DatabaseDescriptor.loadYaml();
        GossiperStubGroupBuilder stubGroupBuilder = new GossiperStubGroupBuilder();
        List<InetAddress> addressList = new LinkedList<InetAddress>();
        for (int i = 0; i < 10; ++i) {
            addressList.add(InetAddress.getByName("127.0.0." + (i + 2)));
        }
        System.out.println(addressList);
        stubGroup = stubGroupBuilder.setClusterId("Test Cluster")
                .setDataCenter("").setNumTokens(1024).setAddressList(addressList)
                .setPartitioner(new Murmur3Partitioner()).build();
        stubGroup.prepareInitialState();
        stubGroup.listen();
        Thread updateThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    Thread.sleep(5000);
                    stubGroup.setupTokenState();
                    stubGroup.setBootStrappingStatusState();
                    Thread.sleep(5000);
                    stubGroup.setNormalStatusState();
                    stubGroup.setSeverityState(0.0);
                    Thread.sleep(5000);
                    stubGroup.setLoad(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });
        updateThread.start();
        while (true) {
            stubGroup.sendGossip(seed);
            stubGroup.updateHeartBeat();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
