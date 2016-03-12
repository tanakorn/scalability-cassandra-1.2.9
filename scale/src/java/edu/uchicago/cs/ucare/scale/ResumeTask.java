package edu.uchicago.cs.ucare.scale;

public abstract class ResumeTask implements Runnable {
    
    private long expectedExecutionTime;
    private long lateness;
    
    public ResumeTask(long expectedExecutionTime) {
        this.expectedExecutionTime = expectedExecutionTime;
        lateness = -1;
    }
    
    @Override
    public void run() {
        lateness = System.currentTimeMillis() - expectedExecutionTime;
        lateness = lateness < 0 ? 0 : lateness;
        resume();
    }
    
    public abstract void resume();
    
    public long getLateness() {
        return lateness;
    }

}
