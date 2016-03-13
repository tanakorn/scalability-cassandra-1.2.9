package edu.uchicago.cs.ucare.scale;

import java.util.Collection;

public interface StubGroup<T extends Stub> extends Iterable<T> {
    
    public Collection<T> getAllStubs();
    public int size();

}
