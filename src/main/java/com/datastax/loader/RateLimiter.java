/*
 * Copyright 2015 Brian Hess
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.loader;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Snapshot;

public class RateLimiter {
    private com.google.common.util.concurrent.RateLimiter rateLimiter;
    private AtomicLong numAcquires;
    private static long updateRate = 100000;
    private Timer timer;
    private PrintStream stream;
    private long lastVal;
    private long firstTime;
    private long lastTime;

    public RateLimiter(double inRate) {
	this(inRate, Long.MAX_VALUE);
    }

    public RateLimiter(double inRate, long inUpdateRate) {
	this(inRate, inUpdateRate, null, null);
    }

    public RateLimiter(double inRate, long inUpdateRate,
		       Timer inTimer, PrintStream inStream) {
	rateLimiter = com.google.common.util.concurrent.RateLimiter.create(inRate);
	updateRate = inUpdateRate;
	timer = inTimer;
	stream = inStream;
	if ((null != stream) && (null != timer)) {
	    printHeader();
	}
	numAcquires = new AtomicLong(0);
	lastTime = System.currentTimeMillis();
	firstTime = lastTime;
	lastVal = 0;
    }

    protected void printHeader() {
	stream.println("Count,Min,Max,Mean,StdDev,50th,75th,95th,98th,99th,999th,MeanRate,1MinuteRate,5MinuteRate,15MinuteRate");
    }

    protected void printStats() {
	Snapshot snapshot = timer.getSnapshot();
	stream.println(String.format("%d,%d,%d,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f",
				     timer.getCount(),
				     snapshot.getMin(),
				     snapshot.getMax(),
				     snapshot.getMean(),
				     snapshot.getStdDev(),
				     snapshot.getMedian(),
				     snapshot.get75thPercentile(),
				     snapshot.get95thPercentile(),
				     snapshot.get98thPercentile(),
				     snapshot.get99thPercentile(),
				     snapshot.get999thPercentile(),
				     timer.getMeanRate(),
				     timer.getOneMinuteRate(),
				     timer.getFiveMinuteRate(),
				     timer.getFifteenMinuteRate())
		       );
    }

    public void report(Long currentVal, Long currentTime) {
	if ((null != stream) && (null != timer)) {
	    printStats();
	    return;
	}
	if (null == currentTime)
	    currentTime = System.currentTimeMillis();
	long etime = (currentTime - firstTime)/1000;
	double rateFromBeginning;
	if (null == currentVal) {
	    currentVal = numAcquires.get() - 1;
	    rateFromBeginning  = (etime > 0) ? (currentVal + 0.0) / etime : 0;
	    System.err.println("Lines Processed: \t" + currentVal 
			       + "  Rate: \t" + rateFromBeginning);
	}
	else {
	    long ltime = (currentTime - lastTime)/1000;
	    double rateFromLast = (ltime > 0) ? (currentVal - lastVal + 0.0) / ltime : 0;
	    rateFromBeginning  = (etime > 0) ? (currentVal + 0.0) / etime : 0;
	    System.err.println("Lines Processed: \t" + currentVal 
			       + "  Rate: \t" + rateFromBeginning
			       + " (" + rateFromLast
			       + ")"
			       );
	}
    }

    protected synchronized void incrementAndReport(int permits) {
	long currentVal = numAcquires.addAndGet(permits);
	long currentTime = System.currentTimeMillis();
	if (permits > currentVal % updateRate) {
	    report(currentVal, currentTime);
	    lastTime = currentTime;
	    lastVal = currentVal;
	}
    }

    public void acquire() {
	rateLimiter.acquire();
	incrementAndReport(1);
    }

    public void acquire(int permits) {
	rateLimiter.acquire(permits);
	incrementAndReport(permits);
    }

    public double getRate() {
	return rateLimiter.getRate();
    }

    public synchronized void setRate(double permitsPerSecond) {
	rateLimiter.setRate(permitsPerSecond);
    }

    public String toString() {
	return rateLimiter.toString();
    }

    public boolean tryAcquire() {
	if (rateLimiter.tryAcquire()) {
	    incrementAndReport(1);
	    return true;
	}
	return false;
    }

    public boolean tryAcquire(int permits) {
	if (rateLimiter.tryAcquire(permits)) {
	    incrementAndReport(permits);
	    return true;
	}
	return false;
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
	if (rateLimiter.tryAcquire(permits, timeout, unit)) {
	    incrementAndReport(permits);
	    return true;
	}
	return false;
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) {
	if (rateLimiter.tryAcquire(timeout, unit)) {
	    incrementAndReport(1);
	    return true;
	}
	return false;
    }

    public long numAcquires() {
	return numAcquires.get();
    }
}
