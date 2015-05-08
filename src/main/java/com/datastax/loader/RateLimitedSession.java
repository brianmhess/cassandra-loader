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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;

public class RateLimitedSession extends EnhancedSession {
    RateLimiter rateLimiter;
    public RateLimitedSession(Session inSession, RateLimiter inRateLimiter) {
	super(inSession);
	rateLimiter = inRateLimiter;
    }

    public long numAcquires() {
	return rateLimiter.numAcquires();
    }

    public double getRate() {
	return rateLimiter.getRate();
    }

    public ResultSet execute(Statement statement) {
	rateLimiter.acquire();
	return super.execute(statement);
    }

    public ResultSet execute(String query) {
	rateLimiter.acquire();
	return super.execute(query);
    }

    public ResultSet execute(String query, Object... values) {
	rateLimiter.acquire();
	return super.execute(query, values);
    }

    public ResultSetFuture executeAsync(Statement statement) {
	rateLimiter.acquire();
	return super.executeAsync(statement);
    }

    public ResultSetFuture executeAsync(String query) {
	rateLimiter.acquire();
	return super.executeAsync(query);
    }

    public ResultSetFuture executeAsync(String query, Object... values) {
	rateLimiter.acquire();
	return super.executeAsync(query, values);
    }

}
