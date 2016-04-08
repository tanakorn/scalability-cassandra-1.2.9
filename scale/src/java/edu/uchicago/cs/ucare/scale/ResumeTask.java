package edu.uchicago.cs.ucare.scale;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public abstract class ResumeTask implements Runnable {
    
    private static long totalLateness = 0;
    private static int resumeCount = 0;
    private static long maxLateness = 0;
    
    public static long totalRealSleepTime = 0;
    public static long totalExpectedSleepTime = 0;
    
    protected final long expectedExecutionTime;
    protected long sleepTime;
    private long lateness;
    
    public static List<Long> latenessList = Collections.synchronizedList(new LinkedList<Long>());
    public static List<Double> percentLatenessList = Collections.synchronizedList(new LinkedList<Double>());
    
    public ResumeTask(long expectedExecutionTime, long sleepTime) {
        this.expectedExecutionTime = expectedExecutionTime;
        this.sleepTime = sleepTime;
        lateness = -1;
    }
    
    @Override
    public void run() {
        lateness = System.currentTimeMillis() - expectedExecutionTime;
        assert lateness >= 0;
        latenessList.add(lateness);

        totalLateness += lateness;
        long realSleepTime = sleepTime + lateness;
        if (sleepTime != 0) {
            percentLatenessList.add(((((double) realSleepTime) / (double) sleepTime) - 1) * 100);
        } else {
            percentLatenessList.add(0.0);
        }
        totalRealSleepTime += realSleepTime;
        totalExpectedSleepTime += sleepTime;
//        System.out.println(realSleepTime + " " + sleepTime);
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
