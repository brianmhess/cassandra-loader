package com.datastax.loader;

import java.lang.String;
import java.lang.System;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.ResultSet;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;

public class FutureSet {
    private List<ResultSetFuture> futures;
    private List<String> strings;
    private Semaphore available;
    private int size;
    private PrintWriter badInsertWriter = null;
    private PrintWriter logWriter = null;
    private TimeUnit unit = TimeUnit.SECONDS;
    private long queryTimeout;
    private long maxInsertErrors;
    private long insertErrors;
    private long beginTime = 0;
    private AtomicLong numInserted;

    public FutureSet() {
	this(500, 2, 10);
    }

    public FutureSet(int inSize, long inQueryTimeout, long inMaxInsertErrors) {
	this(inSize, inQueryTimeout, inMaxInsertErrors, new PrintWriter(System.err), null);
    }
    
    public FutureSet(int inSize, long inQueryTimeout, long inMaxInsertErrors, 
		      PrintWriter inLogWriter, BufferedWriter inBadInsertWriter) {
	size = inSize;
	queryTimeout = inQueryTimeout;
	maxInsertErrors = inMaxInsertErrors;
	available = new Semaphore(size, true);
	futures = new ArrayList<ResultSetFuture>(size);
	strings = new ArrayList<String>(size);
	logWriter = inLogWriter;
	badInsertWriter = new PrintWriter(inBadInsertWriter);
	insertErrors = 0;
	beginTime = System.currentTimeMillis();
	numInserted = new AtomicLong(0);
    }

    public boolean add(ResultSetFuture future, final String line) {
	try {
	    available.acquire();
	}
	catch (InterruptedException e) {
	    return false;
	}
	Futures.addCallback(future, new FutureCallback<ResultSet>() {
		@Override
		public void onSuccess(ResultSet rs) {
		    available.release();
		    numInserted.incrementAndGet();
		}
		@Override
		public void onFailure(Throwable t) {
		    if (logWriter != null) {
			logWriter.println("Error inserting: " + t.getMessage());
			t.printStackTrace(logWriter);
		    }
		    if (badInsertWriter != null) {
			badInsertWriter.println(line);
		    }
		    insertErrors++;
		    if (maxInsertErrors <= insertErrors) {
			if (logWriter != null) {
			    logWriter.println("Too many INSERT errors (" + insertErrors + ")... Stopping");
			}
			//throw new Exception("Too many INSERT errors (" + insertErrors + ")... Stopping");
		    }
		}
	    });
	return true;
    }

    public boolean cleanup() {
	try {
            available.acquire(this.size);
        } catch (InterruptedException e) {
            logWriter.println("Interrupted while waiting for requests to complete.\n" + e.getMessage());
	    return false;
        }
	return true;
    }

    public long getNumInserted() {
	return numInserted.get();
    }
}
