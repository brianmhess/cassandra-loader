package com.datastax.loader;

import java.lang.String;
import java.lang.Throwable;
import java.io.PrintStream;
import com.datastax.driver.core.ResultSet;

public class PrintingFutureAction implements FutureAction {
    protected PrintStream logPrinter = null;
    protected PrintStream badInsertPrinter = null;

    public PrintingFutureAction(PrintStream inLogPrinter, PrintStream inBadInsertPrinter) {
	logPrinter = inLogPrinter;
	badInsertPrinter = inBadInsertPrinter;
    }
    
    public void onSuccess(ResultSet rs, String line) {
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
