package edu.uchicago.cs.ucare.cassandra;

import java.io.File;
import java.io.IOException;

public class CassandraProcess {
    
    Process process;
    
    public CassandraProcess(String operatingDir, int nodeId) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(operatingDir + "/onenode_start", nodeId + "");
        processBuilder.directory(new File(operatingDir));
        process = processBuilder.start();
    }
    
    public void terminate() {
        process.destroy();
    }

}
