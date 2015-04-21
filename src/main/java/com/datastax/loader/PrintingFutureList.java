package com.datastax.loader;

import java.lang.System;
import java.io.PrintStream;

import com.datastax.driver.core.ResultSetFuture;

public class PrintingFutureList extends ActionFutureList {
    public PrintingFutureList() {
	this(500, 2, 10);
    }

    public PrintingFutureList(int inSize, long inQueryTimeout, long inMaxInsertErrors) {
	this(inSize, inQueryTimeout, inMaxInsertErrors, System.err, System.err);
    }
    
    public PrintingFutureList(int inSize, long inQueryTimeout, long inMaxInsertErrors, 
		      PrintStream inLogPrinter, PrintStream inBadInsertPrinter) {
	super(inSize, inQueryTimeout, inMaxInsertErrors, new PrintingFutureAction(inLogPrinter, inBadInsertPrinter));
    }
}
