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

import com.datastax.driver.core.Cluster;

public class Latency999RateLimiter extends AbstractDynamicRateLimiter {
    private Cluster cluster;

    public Latency999RateLimiter(double inRate, long inHowOften,
				 double inMaxStat, double inMinStat,
				 double inDownFraction, 
				 double inUpFraction, Cluster inCluster,
				 boolean inInvertLogic) {
	this(inRate, Long.MAX_VALUE, inHowOften, inMaxStat, inMinStat,
	     inDownFraction, inUpFraction, inCluster, inInvertLogic);
    }

    public Latency999RateLimiter(double inRate, long inUpdateRate,
				 long inHowOften, double inMaxStat, 
				 double inMinStat, double inDownFraction, 
				 double inUpFraction, Cluster inCluster,
				 boolean inInvertLogic) {
	super(inRate, inUpdateRate, inHowOften, inMaxStat, inMinStat, 
	      inDownFraction, inUpFraction, inInvertLogic);
	cluster = inCluster;
    }

    protected synchronized double getCurrStat() {
	return cluster.getMetrics().getRequestsTimer().getSnapshot().get999thPercentile();
    }
}
