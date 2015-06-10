package edu.uchicago.cs.ucare.scale;

import java.net.InetAddress;
import java.util.Collection;

public interface InetAddressStubGroup<T extends InetAddressStub> extends StubGroup<T> {
    
    public Collection<InetAddress> getAllInetAddress();
    
    public T getStub(InetAddress address);

}
