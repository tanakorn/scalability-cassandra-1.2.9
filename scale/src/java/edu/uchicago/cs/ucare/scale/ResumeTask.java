package edu.uchicago.cs.ucare.scale;

public abstract class ResumeTask implements Runnable {
    
    private static long totalLateness = 0;
    private static int resumeCount = 0;
    private static long maxLateness = 0;
    
    protected final long expectedExecutionTime;
    private long lateness;
    
    public ResumeTask(long expectedExecutionTime) {
        this.expectedExecutionTime = expectedExecutionTime;
        lateness = -1;
    }
    
    @Override
    public void run() {
        lateness = System.currentTimeMillis() - expectedExecutionTime;
        lateness = lateness < 0 ? 0 : lateness;
        totalLateness += lateness;
        resumeCount += 1;
        if (maxLateness < lateness) {
            maxLateness = lateness;
        }
        resume();
    }
    
    public abstract void resume();
    
    public long getLateness() {
        return lateness;
    }
    
    public long getExpectedExecutionTime() {
        return expectedExecutionTime;
    }
    
    public static long averageLateness() {
        return resumeCount == 0 ? 0 : totalLateness / resumeCount;
    }
    
    public static long maxLateness() {
        return maxLateness;
    }

}
