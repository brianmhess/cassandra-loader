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

public abstract class AbstractDynamicRateLimiter extends RateLimiter {
    protected long lastCheck = 0;
    protected long howOften;
    protected double lastStat;
    protected double maxStat;
    protected double minStat;
    protected double downFraction;
    protected double upFraction;
    protected boolean invertLogic;

    public AbstractDynamicRateLimiter(double inRate, long inHowOften,
                                      double inMaxStat, double inMinStat,
                                      double inDownFraction, 
                                      double inUpFraction, 
                                      boolean inInvertLogic) {
        this(inRate, Long.MAX_VALUE, inHowOften, inMaxStat, inMinStat,
             inDownFraction, inUpFraction, inInvertLogic);
    }

    public AbstractDynamicRateLimiter(double inRate, long inUpdateRate,
                                      long inHowOften, double inMaxStat, 
                                      double inMinStat, double inDownFraction, 
                                      double inUpFraction, 
                                      boolean inInvertLogic) {
        super(inRate);
        howOften = inHowOften;
        maxStat = inMaxStat;
        minStat = inMinStat;
        downFraction = inDownFraction;
        upFraction = inUpFraction;
        invertLogic = inInvertLogic;
    }

    public void acquire() {
        this.acquire(1);
    }

    public synchronized void acquire(int permits) {
        long currTime = System.currentTimeMillis();
        if (currTime - lastCheck > howOften) {
            adjustRate();
            lastCheck = currTime;
        }
        super.acquire(permits);
    }

    protected synchronized void adjustRate() {
        double currStat = getCurrStat();
        if (statTooHigh(currStat)) {
            if (invertLogic)
                adjustRateUp();
            else
                adjustRateDown();
            System.err.println("Adjusting rate down : " + currStat + " > " + maxStat + "   " + super.getRate());
        }
        else if (statTooLow(currStat)) {
            if (invertLogic)
                adjustRateDown();
            else
                adjustRateUp();
            System.err.println("Adjusting rate up : " + currStat + " > " + maxStat + "   " + super.getRate());
        }
    }

    protected synchronized boolean statTooHigh(double currStat) {
        return currStat > maxStat;
    }

    protected synchronized boolean statTooLow(double currStat) {
        return currStat < minStat;
    }

    protected synchronized void adjustRateDown() {
        double currRate = super.getRate();
        super.setRate(currRate - (currRate * downFraction));
    }

    protected synchronized void adjustRateUp() {
        double currRate = super.getRate();
        super.setRate(currRate + (currRate * upFraction));
    }

    protected abstract double getCurrStat();
}
