package com.datastax.loader.futures;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;
import com.datastax.driver.core.ResultSet;

public class JsonPrintingFutureAction extends PrintingFutureAction {
    private boolean firstBad = true;
    private String badDelim = "[\n";
    public JsonPrintingFutureAction(PrintStream inLogPrinter, 
                                    PrintStream inBadInsertPrinter) {
        super(inLogPrinter, inBadInsertPrinter);
    }
    
    public void onFailure(Throwable t, String line) {
        if (logPrinter != null) {
            logPrinter.println("Error inserting: " + t.getMessage());
            t.printStackTrace(logPrinter);
        }
        if (badInsertPrinter != null) {
            badInsertPrinter.println(badDelim + line);
            if (firstBad) {
                firstBad = false;
                badDelim = ",\n";
            }
        }
    }
}
