package edu.uchicago.cs.ucare.scale;

import java.util.Collection;

public interface StubGroup<T extends Stub> {
    
    public Collection<T> getAllStubs();

}
