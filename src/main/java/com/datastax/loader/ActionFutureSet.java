package com.datastax.loader;

import java.lang.String;
import java.lang.System;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.ResultSet;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;

public class ActionFutureSet extends AbstractFutureManager {
    protected FutureAction futureAction = null;
    protected Semaphore available;
    protected AtomicLong insertErrors;
    protected AtomicLong numInserted;

    public ActionFutureSet(int inSize, long inQueryTimeout, long inMaxInsertErrors, FutureAction inFutureAction) {
	super(inSize, inQueryTimeout, inMaxInsertErrors);
	futureAction = inFutureAction;
	available = new Semaphore(size, true);
	insertErrors = new AtomicLong(0);
	numInserted = new AtomicLong(0);
    }

    public boolean add(ResultSetFuture future, final String line) {
	if (maxInsertErrors <= insertErrors.get())
	    return false;
	final long beginTime = System.currentTimeMillis();
	try {
	    available.acquire();
	    long duration = System.currentTimeMillis() - beginTime;
	    //if (1000 < duration) {
	    //System.err.println("Getting semaphore took " + duration + "ms");
	    //}
	}
	catch (InterruptedException e) {
	    return false;
	}
	Futures.addCallback(future, new FutureCallback<ResultSet>() {
		@Override
		public void onSuccess(ResultSet rs) {
		    available.release();
		    numInserted.incrementAndGet();
		    //long qduration = System.currentTimeMillis() - beginTime;
		    //if (2000 < qduration) {
		    //System.err.println("Query took " + qduration + "ms");
		    //}
		    futureAction.onSuccess(rs, line);
		}
		@Override
		public void onFailure(Throwable t) {
		    available.release();
		    long numErrors = insertErrors.incrementAndGet();
		    futureAction.onFailure(t, line);
		    if (maxInsertErrors <= numErrors) {
			futureAction.onTooManyFailures();
		    }
		}
	    });
	return true;
    }

    public boolean cleanup() {
	try {
            available.acquire(this.size);
        } catch (InterruptedException e) {
	    return false;
        }
	return true;
    }

    public long getNumInserted() {
	return numInserted.get();
    }
}
