package edu.uchicago.cs.ucare.scale;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;

public abstract class InetAddressStubGroup implements ScaleStubGroup {
    
    Map<InetAddress, ScaleStub> stubs;
    
    public Collection<InetAddress> getAllInetAddress() {
        return stubs.keySet();
    }

    @Override
    public Collection<ScaleStub> getAllStubs() {
        return stubs.values();
    }
    
}
