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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RateLimiter {
    private com.google.common.util.concurrent.RateLimiter rateLimiter;
    private AtomicLong numAcquires;
    private static long updateRate = 100000;
    private long lastVal;
    private long firstTime;
    private long lastTime;

    public RateLimiter(double inRate) {
	this(inRate, Long.MAX_VALUE);
    }

    public RateLimiter(double inRate, long inUpdateRate) {
	rateLimiter = com.google.common.util.concurrent.RateLimiter.create(inRate);
	updateRate = inUpdateRate;
	numAcquires = new AtomicLong(0);
	lastTime = System.currentTimeMillis();
	firstTime = lastTime;
	lastVal = 0;
    }

    protected synchronized void incrementAndReport(int permits) {
	long currentVal = numAcquires.addAndGet(permits);
	long currentTime = System.currentTimeMillis();
	if (0 == currentVal % updateRate) {
	    System.err.println("Lines Processed: \t" + currentVal 
			       + "  Rate: \t" + 
			       (currentVal)
			       /((currentTime - firstTime) / 1000)
			       + " (" + 
			       (currentVal - lastVal) 
			       / ((currentTime - lastTime) / 1000)
			       + ")"
			       );
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
