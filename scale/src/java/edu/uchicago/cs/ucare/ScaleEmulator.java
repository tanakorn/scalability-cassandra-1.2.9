package edu.uchicago.cs.ucare;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;

public class ScaleEmulator {
	
	public static GossiperStub stub;

	public static void main(String[] args) throws UnknownHostException, ConfigurationException, InterruptedException {
        InetAddress seed = InetAddress.getByName("127.0.0.1");
		stub = new GossiperStub("Test Cluster", "datacenter1", InetAddress.getByName("127.0.0.2"), 1024, new Murmur3Partitioner());
		stub.prepareInitialState();
		stub.setupTokenState();
		stub.setBootStrappingStatusState();
		stub.setLoad(10000);
		stub.setSeverityState(0.0);
		stub.listen();
        stub.updateHeartBeat();
		while (true) {
            stub.doGossip(seed);
            Thread.sleep(1000);
            stub.updateHeartBeat();
		}
	}

}
