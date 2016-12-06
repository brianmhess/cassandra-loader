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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.loader.futures.FutureManager;
import com.datastax.loader.futures.PrintingFutureSet;
import com.datastax.loader.futures.JsonPrintingFutureSet;
import com.datastax.loader.parser.BooleanParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

class CqlDelimLoadTask implements Callable<Long> {
    private String BADPARSE = ".BADPARSE";
    private String BADINSERT = ".BADINSERT";
    private String LOG = ".LOG";
    private Session session;
    private String insert;
    private PreparedStatement statement;
    private BatchStatement batch;
    private StringBuilder batchString;
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
    private int charsPerColumn = 4096;
    private TimeUnit unit = TimeUnit.SECONDS;
    private long queryTimeout = 2;
    private int numRetries = 1;
    private long maxInsertErrors = 10;
    private long insertErrors = 0;
    private boolean nullsUnset;
    private String format = "delim";
    private String keyspace = null;
    private String table = null;
    private JSONArray jsonArray;
    private boolean fuzzyMatch;

    public CqlDelimLoadTask(String inCqlSchema, String inDelimiter, 
                            int inCharsPerColumn,
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
                            String inKeyspace, String inTable, boolean inFuzzyMatch) {
        super();
        cqlSchema = inCqlSchema;
        delimiter = inDelimiter;
        charsPerColumn = inCharsPerColumn;
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
        fuzzyMatch = inFuzzyMatch;
    }

    public Long call() throws IOException, ParseException, org.json.simple.parser.ParseException {
        setup();
        numInserted = execute();
        return numInserted;
    }

    private void setup() throws IOException, ParseException, org.json.simple.parser.ParseException {
        if (null == infile) {
            reader = new BufferedReader(new InputStreamReader(System.in));
            readerName = "stdin";
        }
        else {
            reader = new BufferedReader(new FileReader(infile));
            readerName = infile.getName();
        }

        //setup json reader
        if(format.equalsIgnoreCase("jsonarray")){
            JSONParser parser = new JSONParser();
            jsonArray= (JSONArray) parser.parse(reader);
        }

        // Prepare Badfile
        if (null != badDir) {
            badParsePrinter = new PrintStream(new BufferedOutputStream(new FileOutputStream(badDir + "/" + readerName + BADPARSE)));
            badInsertPrinter = new PrintStream(new BufferedOutputStream(new FileOutputStream(badDir + "/" + readerName + BADINSERT)));
            logFname = badDir + "/" + readerName + LOG;
            logPrinter = new PrintStream(new BufferedOutputStream(new FileOutputStream(logFname)));
        }



        if (keyspace == null) {
            cdp = new CqlDelimParser(cqlSchema, delimiter, charsPerColumn, 
                                     nullString,
                                     dateFormatString, boolStyle, locale,
                                     skipCols, session, true);
        }
        else{
            cdp = new CqlDelimParser(keyspace, table, delimiter, charsPerColumn,
                                     nullString, 
                                     dateFormatString, boolStyle, locale, 
                                     skipCols, session, true);
            if(fuzzyMatch){
                //TODO: is this a good read ahead limit?
                reader.mark(20000);
                String cqlSchema = new CqlSchemaFuzzyMatcher().match(keyspace, table, reader, delimiter, cdp);
                reader.reset();
                cdp = new CqlDelimParser(cqlSchema, delimiter, charsPerColumn,
                        nullString,
                        dateFormatString, boolStyle, locale,
                        skipCols, session, true);
            }
        }

        insert = cdp.generateInsert();
        statement = session.prepare(insert);
        statement.setRetryPolicy(new LoaderRetryPolicy(numRetries));
        statement.setConsistencyLevel(consistencyLevel);
        batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        batchString = new StringBuilder();
        if (format.equalsIgnoreCase("delim")) {
            fm = new PrintingFutureSet(numFutures, queryTimeout, 
                                       maxInsertErrors, logPrinter, 
                                       badInsertPrinter);
        }
        else if (format.equalsIgnoreCase("jsonline")
                 || format.equalsIgnoreCase("jsonarray")) {
            fm = new JsonPrintingFutureSet(numFutures, queryTimeout, 
                                       maxInsertErrors, logPrinter, 
                                       badInsertPrinter);
        }
    }
        
    private void cleanup(boolean success) throws IOException {
        if (null != badParsePrinter) {
            if (format.equalsIgnoreCase("jsonarray"))
                badParsePrinter.println("]");
            badParsePrinter.close();
        }
        if (null != badInsertPrinter) {
            if (format.equalsIgnoreCase("jsonarray"))
                badInsertPrinter.println("]");
            badInsertPrinter.close();
        }
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

    private int sendInsert(List<Object> elements, String line) {
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
            batchString.append("\n").append(line);
            if (batchSize == batch.size()) {
                resultSetFuture = session.executeAsync(batch);
                if (!fm.add(resultSetFuture, batchString.toString())) {
                    System.err.println("There was an error.  Please check the log file for more information (" + logFname + ")");
                    //cleanup(false);
                    return -2;
                }
                int numInserted = batch.size();
                batch.clear();
                batchString.setLength(0);
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
        if (format.equalsIgnoreCase("delim")
            || format.equalsIgnoreCase("jsonline")) {
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

                elements = null;
                if (format.equalsIgnoreCase("delim"))
                    elements = cdp.parse(line);
                else if (format.equalsIgnoreCase("jsonline"))
                    elements = cdp.parseJson(line);
                if (null != elements) {
                    int ret = sendInsert(elements, line);
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
        else if (format.equalsIgnoreCase("jsonarray")) {
            boolean firstBadJson = true;
            String badJsonDelim = "[\n";
            List<String> columnBackbone = cdp.getColumnNames();
            int columnCount = columnBackbone.size();
            for (Object o : jsonArray) {
                JSONObject jsonRow = (JSONObject) o;
                String[] jsonElements = new String[columnCount];
                jsonElements[0] = jsonRow.get(columnBackbone.get(0)).toString();
                for (int i = 1; i < columnCount; i++) {
                    if (null != jsonRow.get(columnBackbone.get(i))) {
                        jsonElements[i] = jsonRow.get(columnBackbone.get(i)).toString();
                    } else {
                        jsonElements[i] = null;
                    }
                }
                if (null != (elements = cdp.parse(jsonElements))) {
                    int ret = sendInsert(elements, line);
                    if (-2 == ret) {
                        cleanup(false);
                        return -2;
                    }
                    numInserted += ret;
                } else {
                    String badString = jsonRow.toJSONString();
                    if (null != logPrinter) {
                        logPrinter.println(String.format("Error parsing JSON item %d in %s: %s", lineNumber, readerName, badString));
                    }
                    System.err.println(String.format("Error parsing JSON item %d in %s: %s", lineNumber, readerName, badString));
                    if (null != badParsePrinter) {
                        badParsePrinter.println(badJsonDelim + badString);
                        if (firstBadJson) {
                            firstBadJson = false;
                            badJsonDelim = ",\n";
                        }
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
        }// if (format.equalsIgnoreCase("json"))

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
