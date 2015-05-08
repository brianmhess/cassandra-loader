package com.datastax.loader.futures;

import java.lang.String;
import java.lang.Throwable;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;
import com.datastax.driver.core.ResultSet;

public class PrintingFutureAction implements FutureAction {
    protected PrintStream logPrinter = null;
    protected PrintStream badInsertPrinter = null;
    protected AtomicLong numInserted;
    protected final long period = 100000;

    public PrintingFutureAction(PrintStream inLogPrinter, 
				PrintStream inBadInsertPrinter) {
	logPrinter = inLogPrinter;
	badInsertPrinter = inBadInsertPrinter;
	numInserted = new AtomicLong(0);
    }
    
    public void onSuccess(ResultSet rs, String line) {
	if (logPrinter != null) {
	    long cur = numInserted.incrementAndGet();
	    if (0 == (cur % period)) {
		logPrinter.println("Progress:  " + cur);
	    }
	}   
    }

    public void onFailure(Throwable t, String line) {
	if (logPrinter != null) {
	    logPrinter.println("Error inserting: " + t.getMessage());
	    t.printStackTrace(logPrinter);
	}
	if (badInsertPrinter != null) {
	    badInsertPrinter.println(line);
	}
    }

    public void onTooManyFailures() {
	if (logPrinter != null) {
	    logPrinter.println("Too many INSERT errors ... Stopping");
	}
    }

}
