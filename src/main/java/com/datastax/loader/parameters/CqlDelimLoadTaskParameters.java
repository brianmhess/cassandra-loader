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

public CqlDelimLoadTaskParameters {
    private int numFutures = 1000;
    private int inNumFutures = -1;
    private int queryTimeout = 2;
    private long maxInsertErrors = 10;
    private long maxErrors = 10;
    private long skipRows = 0;
    private long maxRows = -1;
    private String badDir = ".";
    private String successDir = null;
    private String failureDir = null;
    private int batchSize = 1;
    private boolean nullsUnset = false;

    private CqlDelimParserParameters cqlDelimParserParameters = new CqlDelimParserParameters();
    private CassandrastatementParameters cassandraStatementParameters = new CassandraStatementParameters();

    public CqlDelimLoadTaskParameters() { }

    public boolean parseArgs(Map<String,String> amap) {
        if (null != (tkey = amap.remove("-numFutures")))    inNumFutures = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-batchSize")))     batchSize = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-queryTimeout")))  queryTimeout = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-maxInsertErrors"))) maxInsertErrors = Long.parseLong(tkey);
        if (null != (tkey = amap.remove("-maxErrors")))     maxErrors = Long.parseLong(tkey);
        if (null != (tkey = amap.remove("-skipRows")))      skipRows = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-maxRows")))       maxRows = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-badDir")))        badDir = tkey;
        if (null != (tkey = amap.remove("-successDir")))    successDir = tkey;
        if (null != (tkey = amap.remove("-failureDir")))    failureDir = tkey;
        if (null != (tkey = amap.remove("-nullsUnset")))    nullsUnset = Boolean.parseBoolean(tkey);
        if (-1 == maxRows)
            maxRows = Long.MAX_VALUE;
        if (-1 == maxErrors)
            maxErrors = Long.MAX_VALUE;
        if (-1 == maxInsertErrors)
            maxInsertErrors = Long.MAX_VALUE;

        if (0 < inNumFutures)
            numFutures = inNumFutures / numThreads;

        if (!cqlDelimParserParameters.parseArgs(amap))
            return false;
        if (!cassandraStatementParameters.parseArgs(amap))
            return false;
        return validateArgs();
    }

    public boolean validateArgs() {
        if (0 >= numFutures) {
            System.err.println("Number of futures must be positive (" + numFutures + ")");
            return false;
        }
        if (0 >= batchSize) {
            System.err.println("Batch size must be positive (" + batchSize + ")");
            return false;
        }
        if (0 >= queryTimeout) {
            System.err.println("Query timeout must be positive");
            return false;
        }
        if (0 > maxInsertErrors) {
            System.err.println("Maximum number of insert errors must be non-negative");
            return false;
        }
        if (0 > skipRows) {
            System.err.println("Number of rows to skip must be non-negative");
            return false;
        }
        if (0 >= maxRows) {
            System.err.println("Maximum number of rows to load must be positive");
            return false;
        }
        if (0 > maxErrors) {
            System.err.println("Maximum number of parse errors must be non-negative");
            return false;
        }
        if (null != successDir) {
            if (STDIN.equalsIgnoreCase(filename)) {
                System.err.println("Cannot specify -successDir with stdin");
                return false;
            }
            File sdir = new File(successDir);
            if (!sdir.isDirectory()) {
                System.err.println("-successDir must be a directory");
                return false;
            }
        }
        if (null != failureDir) {
            if (STDIN.equalsIgnoreCase(filename)) {
                System.err.println("Cannot specify -failureDir with stdin");
                return false;
            }
            File sdir = new File(failureDir);
            if (!sdir.isDirectory()) {
                System.err.println("-failureDir must be a directory");
                return false;
            }
        }

        return true;
    }

    public String usage() {
        StringBuilder usage = new StringBuilder();
        usage.append("  -skipRows <skipRows>               Number of rows to skip [0]\n");
        usage.append("  -maxRows <maxRows>                 Maximum number of rows to read (-1 means all) [-1]\n");
        usage.append("  -maxErrors <maxErrors>             Maximum parse errors to endure [10]\n");
        usage.append("  -badDir <badDirectory>             Directory for where to place badly parsed rows. [none]\n");
        usage.append("  -numFutures <numFutures>           Number of CQL futures to keep in flight [1000]\n");
        usage.append("  -batchSize <batchSize>             Number of INSERTs to batch together [1]\n");
        usage.append("  -queryTimeout <# seconds>          Query timeout (in seconds) [2]\n");
        usage.append("  -maxInsertErrors <# errors>        Maximum INSERT errors to endure [10]\n");
        usage.append("  -successDir <dir>                  Directory where to move successfully loaded files\n");
        usage.append("  -failureDir <dir>                  Directory where to move files that did not successfully load\n");
        usage.append("  -nullsUnset [false|true]           Treat nulls as unset [faslse]\n");

        usage.append(cqlDelimParserParameters.usage());
        usage.append(cassandraStatementParameters.usage());
        return usage.toString();
    }
}
