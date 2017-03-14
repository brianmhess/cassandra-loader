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
package com.datastax.loader.parameters;

public CassandraStatementParameters {
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_ONE;
    private int numRetries = 1;

    public CassandrastatementParameters() { }

    public boolean parseArgs(Map<String,String> amap) {
        String tkey;

        return validateArgs();
    }

    public boolean validateArgs() {
        if (0 > numRetries) {
            System.err.println("Number of retries must be non-negative");
            return false;
        }

        return true;
    }

    public String usage() {
        StringBuilder usage = new StringBuilder(" Cassandra Statement Parameters:\n");
        usage.append("  -consistencyLevel <CL>             Consistency level [LOCAL_ONE]\n");
        usage.append("  -numRetries <numRetries>           Number of times to retry the INSERT [1]\n");

        return usage.toString();
    }

    public boolean setCassandraStatementParameters(Statement statement) {
        statement.setRetryPolicy(new LoaderRetryPolicy(numRetries));
        statement.setConsistencyLevel(consistencyLevel);
        return true;
    }
}
