package com.datastax.loader;

import java.lang.String;
import java.lang.System;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ResultSetFuture;

public class FutureList {
    private List<ResultSetFuture> futures;
    private List<String> strings;
    private int size;
    private PrintWriter badInsertWriter = null;
    private PrintWriter logWriter = null;
    private TimeUnit unit = TimeUnit.SECONDS;
    private long queryTimeout;
    private long maxInsertErrors;
    private long insertErrors;
    private long beginTime = 0;
    private long numInserted;

    public FutureList() {
	this(500, 2, 10);
    }

    public FutureList(int inSize, long inQueryTimeout, long inMaxInsertErrors) {
	this(inSize, inQueryTimeout, inMaxInsertErrors, new PrintWriter(System.err), null);
    }
    
    public FutureList(int inSize, long inQueryTimeout, long inMaxInsertErrors, 
		      PrintWriter inLogWriter, BufferedWriter inBadInsertWriter) {
	size = inSize;
	queryTimeout = inQueryTimeout;
	maxInsertErrors = inMaxInsertErrors;
	futures = new ArrayList<ResultSetFuture>(size);
	strings = new ArrayList<String>(size);
	logWriter = inLogWriter;
	badInsertWriter = new PrintWriter(inBadInsertWriter);
	insertErrors = 0;
	beginTime = System.currentTimeMillis();
	numInserted = 0;
    }

    public boolean add(ResultSetFuture future, String line) {
	if (futures.size() >= size) {
	    if (!purgeFutures())
		return false;
	}
	futures.add(future);
	strings.add(line);
	numInserted++;
	return true;
    }

    public boolean purgeFutures() {
	if (0 == futures.size())
	    return true;
	for (int i = 0; i < futures.size(); i++) {
	    ResultSetFuture future = futures.get(i);
	    String line = strings.get(i);
	    try {
		if (null != future)
		    future.getUninterruptibly(queryTimeout, unit);
	    }
	    catch (Exception e) {
		if (logWriter != null) {
		    logWriter.println("Error inserting: " + e.getMessage());
		    e.printStackTrace(logWriter);
		}
		if (badInsertWriter != null) {
		    badInsertWriter.println(line);
		}
		insertErrors++;
		if (maxInsertErrors <= insertErrors) {
		    if (logWriter != null) {
			logWriter.println("Too many INSERT errors (" + insertErrors + ")... Stopping");
		    }
		    return false;
		}
	    }
	}
	futures.clear();
	return true;
    }

    public boolean cleanup() {
	return purgeFutures();
    }

    public long getNumInserted() {
	return numInserted;
    }
}
