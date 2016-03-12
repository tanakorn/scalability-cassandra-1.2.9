package edu.uchicago.cs.ucare.scale;

import java.util.TimerTask;

public abstract class ResumeTask extends TimerTask {
    
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
