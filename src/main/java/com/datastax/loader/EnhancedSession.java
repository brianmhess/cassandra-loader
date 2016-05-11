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

import java.util.Map;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.PreparedStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class EnhancedSession implements Session {
    private Session session;
    public EnhancedSession(Session inSession) {
	session = inSession;
    }

    public void close() {
	session.close();
    }

    public CloseFuture closeAsync() {
	return session.closeAsync();
    }

    public ResultSet execute(Statement statement) {
	return session.execute(statement);
    }

    public ResultSet execute(String query) {
	return session.execute(query);
    }

    public ResultSet execute(String query, Object... values) {
	return session.execute(query, values);
    }

    public ResultSet execute(String query, Map<String,Object> values) {
	return session.execute(query, values);
    }

    public ResultSetFuture executeAsync(Statement statement) {
	return session.executeAsync(statement);
    }

    public ResultSetFuture executeAsync(String query) {
	return session.executeAsync(query);
    }

    public ResultSetFuture executeAsync(String query, Object... values) {
	return session.executeAsync(query, values);
    }

    public ResultSetFuture executeAsync(String query, Map<String,Object> values) {
	return session.executeAsync(query, values);
    }

    public Cluster getCluster() {
	return session.getCluster();
    }

    public String getLoggedKeyspace() {
	return session.getLoggedKeyspace();
    }

    public Session.State getState() {
	return session.getState();
    }

    public EnhancedSession init() {
	session.init();
	return this;
    }

    public com.google.common.util.concurrent.ListenableFuture<Session> initAsync() {
	return session.initAsync();
    }

    public boolean isClosed() {
	return session.isClosed();
    }

    public PreparedStatement prepare(RegularStatement statement) {
	return session.prepare(statement);
    }

    public PreparedStatement prepare(String query) {
	return session.prepare(query);
    }

    public com.google.common.util.concurrent.ListenableFuture<PreparedStatement>
	prepareAsync(RegularStatement statement) {
	return session.prepareAsync(statement);
    }

    public com.google.common.util.concurrent.ListenableFuture<PreparedStatement>
	prepareAsync(String query) {
	return session.prepareAsync(query);
    }
}
