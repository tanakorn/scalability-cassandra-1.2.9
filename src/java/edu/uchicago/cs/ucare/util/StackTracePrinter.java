package edu.uchicago.cs.ucare.util;

import org.slf4j.Logger;

public class StackTracePrinter {
	
	public static void print(Logger logger) {
		StringBuilder resultBuilder = new StringBuilder("Print stack trace\n");
    	StackTraceElement[] trace= Thread.currentThread().getStackTrace();
    	resultBuilder.append(trace[2].getClassName());
    	resultBuilder.append('.');
    	resultBuilder.append(trace[2].getMethodName());
    	for (int i = 2; i < trace.length; ++i) {
            resultBuilder.append('\n');
    		resultBuilder.append(trace[i].toString());
    	}
    	logger.info(resultBuilder.toString());
	}

}
