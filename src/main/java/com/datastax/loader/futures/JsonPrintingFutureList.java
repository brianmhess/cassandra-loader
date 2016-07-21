package com.datastax.loader.futures;

import java.io.PrintStream;

import com.datastax.driver.core.ResultSetFuture;

public class JsonPrintingFutureList extends ActionFutureList {
    public JsonPrintingFutureList() {
        this(500, 2, 10);
    }

    public JsonPrintingFutureList(int inSize, long inQueryTimeout, 
                                  long inMaxInsertErrors) {
        this(inSize, inQueryTimeout, inMaxInsertErrors, System.err, System.err);
    }
    
    public JsonPrintingFutureList(int inSize, long inQueryTimeout, 
                                  long inMaxInsertErrors, 
                                  PrintStream inLogPrinter, 
                                  PrintStream inBadInsertPrinter) {
        super(inSize, inQueryTimeout, inMaxInsertErrors, 
              new JsonPrintingFutureAction(inLogPrinter, inBadInsertPrinter));
    }
}
