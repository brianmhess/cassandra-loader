package com.datastax.loader.futures;

import java.io.PrintStream;

public class JsonPrintingFutureSet extends ActionFutureSet {
    public JsonPrintingFutureSet() {
        this(500, 2, 10);
    }

    public JsonPrintingFutureSet(int inSize, long inQueryTimeout, 
                                 long inMaxInsertErrors) {
        this(inSize, inQueryTimeout, inMaxInsertErrors, System.err, System.err);
    }
    
    public JsonPrintingFutureSet(int inSize, long inQueryTimeout, 
                                 long inMaxInsertErrors, 
                                 PrintStream inLogPrinter, 
                                 PrintStream inBadInsertPrinter) {
        super(inSize, inQueryTimeout, inMaxInsertErrors, 
              new JsonPrintingFutureAction(inLogPrinter, inBadInsertPrinter));
    }
}
