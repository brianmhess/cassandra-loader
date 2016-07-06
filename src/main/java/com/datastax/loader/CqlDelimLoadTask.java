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

import com.datastax.loader.parser.BooleanParser;
import com.datastax.loader.futures.FutureManager;
import com.datastax.loader.futures.PrintingFutureSet;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.text.ParseException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

class CqlDelimLoadTask implements Callable<Long> {
    private String BADPARSE = ".BADPARSE";
    private String BADINSERT = ".BADINSERT";
    private String LOG = ".LOG";
    private Session session;
    private String insert;
    private PreparedStatement statement;
    private BatchStatement batch;
    private FutureManager fm;
    private ConsistencyLevel consistencyLevel;
    private CqlDelimParser cdp;
    private long maxErrors;
    private long skipRows;
    private String skipCols = null;
    private long maxRows;
    private String badDir;
    private String successDir;
    private String failureDir;
    private String readerName;
    private PrintStream badParsePrinter = null;
    private PrintStream badInsertPrinter = null;
    private PrintStream logPrinter = null;
    private String logFname = "";
    private BufferedReader reader;
    private File infile;
    private int numFutures;
    private int batchSize;
    private long numInserted;

    private String cqlSchema;
    private Locale locale = null;
    private BooleanParser.BoolStyle boolStyle = null;
    private String dateFormatString = null;
    private String nullString = null;
    private String delimiter = null;
    private TimeUnit unit = TimeUnit.SECONDS;
    private long queryTimeout = 2;
    private int numRetries = 1;
    private long maxInsertErrors = 10;
    private long insertErrors = 0;
    private boolean nullsUnset;
    private String format = "delim";
    private String keyspace = null;
    private String table = null;

    public CqlDelimLoadTask(String inCqlSchema, String inDelimiter, 
                            String inNullString, String inDateFormatString,
                            BooleanParser.BoolStyle inBoolStyle, 
                            Locale inLocale, 
                            long inMaxErrors, long inSkipRows, 
                            String inSkipCols, long inMaxRows,
                            String inBadDir, File inFile,
                            Session inSession, ConsistencyLevel inCl,
                            int inNumFutures, int inBatchSize, int inNumRetries, 
                            int inQueryTimeout, long inMaxInsertErrors,
                            String inSuccessDir, String inFailureDir,
                            boolean inNullsUnset, String inFormat,
                            String inKeyspace, String inTable) {
        super();
        cqlSchema = inCqlSchema;
        delimiter = inDelimiter;
        nullString = inNullString;
        dateFormatString = inDateFormatString;
        boolStyle = inBoolStyle;
        locale = inLocale;
        maxErrors = inMaxErrors;
        skipRows = inSkipRows;
        skipCols = inSkipCols;
        maxRows = inMaxRows;
        badDir = inBadDir;
        infile = inFile;
        session = inSession;
        consistencyLevel = inCl;
        numFutures = inNumFutures;
        batchSize = inBatchSize;
        numRetries = inNumRetries;
        queryTimeout = inQueryTimeout;
        maxInsertErrors = inMaxInsertErrors;
        successDir = inSuccessDir;
        failureDir = inFailureDir;
        nullsUnset = inNullsUnset;
        format = inFormat;
        keyspace = inKeyspace;
        table = inTable;
    }

    public Long call() throws IOException, ParseException {
        setup();
        numInserted = execute();
        return numInserted;
    }

    private void setup() throws IOException, ParseException {
        if (null == infile) {
            reader = new BufferedReader(new InputStreamReader(System.in));
            readerName = "stdin";
        }
        else {
            reader = new BufferedReader(new FileReader(infile));
            readerName = infile.getName();
        }
            
        // Prepare Badfile
        if (null != badDir) {
            badParsePrinter = new PrintStream(new BufferedOutputStream(new FileOutputStream(badDir + "/" + readerName + BADPARSE)));
            badInsertPrinter = new PrintStream(new BufferedOutputStream(new FileOutputStream(badDir + "/" + readerName + BADINSERT)));
            logFname = badDir + "/" + readerName + LOG;
            logPrinter = new PrintStream(new BufferedOutputStream(new FileOutputStream(logFname)));
        }
            
        cdp = new CqlDelimParser(cqlSchema, delimiter, nullString, 
                                 dateFormatString, boolStyle, locale, 
                                 skipCols, session, true);
        insert = cdp.generateInsert();
        statement = session.prepare(insert);
        statement.setRetryPolicy(new LoaderRetryPolicy(numRetries));
        statement.setConsistencyLevel(consistencyLevel);
        batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        fm = new PrintingFutureSet(numFutures, queryTimeout, 
                                   maxInsertErrors, logPrinter, 
                                   badInsertPrinter);
    }
        
    private void cleanup(boolean success) throws IOException {
        if (null != badParsePrinter)
            badParsePrinter.close();
        if (null != badInsertPrinter)
            badInsertPrinter.close();
        if (null != logPrinter)
            logPrinter.close();
        if (success) {
            if (null != successDir) {
                Path src = infile.toPath();
                Path dst = Paths.get(successDir);
                Files.move(src, dst.resolve(src.getFileName()), 
                           StandardCopyOption.REPLACE_EXISTING);
            }
        }
        else {
            if (null != failureDir) {
                Path src = infile.toPath();
                Path dst = Paths.get(failureDir);
                Files.move(src, dst.resolve(src.getFileName()), 
                           StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private int sendInserts(List<Object> elements, String line) {
        BoundStatement bind = statement.bind(elements.toArray());
        ResultSetFuture resultSetFuture;
        int retval = 0;
        if (nullsUnset) {
            for (int i = 0; i < elements.size(); i++)
                if (null == elements.get(i))
                    bind.unset(i);
        }
        if (1 == batchSize) {
            resultSetFuture = session.executeAsync(bind);
            if (!fm.add(resultSetFuture, line)) {
                System.err.println("There was an error.  Please check the log file for more information (" + logFname + ")");
                //cleanup(false);
                return -2;
            }
            //numInserted += 1;
            retval = 1;
        }
        else {
            batch.add(bind);
            if (batchSize == batch.size()) {
                resultSetFuture = session.executeAsync(batch);
                if (!fm.add(resultSetFuture, line)) {
                    System.err.println("There was an error.  Please check the log file for more information (" + logFname + ")");
                    //cleanup(false);
                    return -2;
                }
                int numInserted = batch.size();
                batch.clear();
                retval = numInserted;
            }
        }
        return retval;
    }

    private long execute() throws IOException {
        String line = null;
        int lineNumber = 0;
        long numInserted = 0;
        int numErrors = 0;
        int curBatch = 0;
        BoundStatement bind = null;
        List<Object> elements = null;

        System.err.println("*** Processing " + readerName);
        if (format.equalsIgnoreCase("delim")) {
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (skipRows > 0) {
                    skipRows--;
                    continue;
                }
                if (maxRows-- < 0)
                    break;
                
                if (0 == line.trim().length())
                    continue;
                
                if (null != (elements = cdp.parse(line))) {
                    int ret = sendInserts(elements, line);
                    if (-2 == ret) {
                        cleanup(false);
                        return -2;
                    }
                    numInserted += ret;
                }
                else {
                    if (null != logPrinter) {
                        logPrinter.println(String.format("Error parsing line %d in %s: %s", lineNumber, readerName, line));
                    }
                    System.err.println(String.format("Error parsing line %d in %s: %s", lineNumber, readerName, line));
                    if (null != badParsePrinter) {
                        badParsePrinter.println(line);
                    }
                    numErrors++;
                    if (maxErrors <= numErrors) {
                        if (null != logPrinter) {
                            logPrinter.println(String.format("Maximum number of errors exceeded (%d) for %s", numErrors, readerName));
                        }
                        System.err.println(String.format("Maximum number of errors exceeded (%d) for %s", numErrors, readerName));
                        cleanup(false);
                        return -1;
                    }
                }
            }
        } // if (format.equalsIgnoreCase("delim"))
        else if (format.equalsIgnoreCase("json")) {

            System.err.println("JSON NOT YET IMPLEMENTED!");

            // JSON code to get List<Object> elements put JSON string in line
            int ret = sendInserts(elements, line);
            if (-2 == ret) {
                cleanup(false);
                return -2;
            }
            numInserted += ret;
        }

        // Send last partially filled batch
        if ((batchSize > 1) && (batch.size() > 0)) {
            ResultSetFuture resultSetFuture = session.executeAsync(batch);
            if (!fm.add(resultSetFuture, line)) {
                cleanup(false);
                return -2;
            }
            numInserted += batch.size();
        }

        if (!fm.cleanup()) {
            cleanup(false);
            return -1;
        }

        if (null != logPrinter) {
            logPrinter.println("*** DONE: " + readerName + "  number of lines processed: " + lineNumber + " (" + numInserted + " inserted)");
        }
        System.err.println("*** DONE: " + readerName + "  number of lines processed: " + lineNumber + " (" + numInserted + " inserted)");

        cleanup(true);
        return fm.getNumInserted();
    }
}
