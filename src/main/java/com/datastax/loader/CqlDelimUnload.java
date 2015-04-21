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
import com.datastax.loader.parser.CqlDelimParser;

import java.lang.System;
import java.lang.String;
import java.lang.Integer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.Locale;
import java.math.BigInteger;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;


public class CqlDelimUnload {
    private String version = "0.0.6";
    private String host = null;
    private int port = 9042;
    private String username = null;
    private String password = null;
    private Cluster cluster = null;
    private Session session = null;
    private String beginToken = "-9223372036854775808";
    private String endToken = "9223372036854775807";

    private String cqlSchema = null;
    private String filename = null;

    private Locale locale = null;
    private BooleanParser.BoolStyle boolStyle = null;
    private String dateFormatString = null;
    private String nullString = null;
    private String delimiter = null;
    private boolean delimiterInQuotes = false;

    private int numThreads = 5;

    private String usage() {
	String usage = "version: " + version + "\n";
	usage = usage + "Usage: -f <outputStem> -host <ipaddress> -schema <schema> [OPTIONS]\n";
	usage = usage + "OPTIONS:\n";
	usage = usage + "  -delim <delimiter>             Delimiter to use [,]\n";
	usage = usage + "  -dateFormat <dateFormatString> Date format [default for Locale.ENGLISH]\n";
	usage = usage + "  -nullString <nullString>       String that signifies NULL [none]\n";
	usage = usage + "  -port <portNumber>             CQL Port Number [9042]\n";
	usage = usage + "  -user <username>               Cassandra username [none]\n";
	usage = usage + "  -pw <password>                 Password for user [none]\n";
	usage = usage + "  -decimalDelim <decimalDelim>   Decimal delimiter [.] Other option is ','\n";
	usage = usage + "  -boolStyle <boolStyleString>   Style for booleans [TRUE_FALSE]\n";
	usage = usage + "  -numThreads <numThreads>       Number of concurrent threads (files) to load [5]\n";
	usage = usage + "  -beginToken <tokenString>      Begin token [none]\n";
	usage = usage + "  -endToken <tokenString>        End token [none]\n";
	return usage;
    }
    
    private boolean validateArgs() {
	if (numThreads < 1) {
	    System.err.println("Number of threads must be non-negative");
	    return false;
	}
	if ((null == username) && (null != password)) {
	    System.err.println("If you supply the password, you must supply the username");
	    return false;
	}
	if ((null != username) && (null == password)) {
	    System.err.println("If you supply the username, you must supply the password");
	    return false;
	}
	if (filename.equalsIgnoreCase("stdout")) {
	    numThreads = 1;
	}
	/*
	if (null == beginToken) {
	    System.err.println("You must specify the begin token");
	    return false;
	}
	if (null == endToken) {
	    System.err.println("You must specify the end token");
	    return false;
	}
	*/

	return true;
    }
    
    private boolean parseArgs(String[] args) {
	if (args.length == 0) {
	    System.err.println("No arguments specified");
	    return false;
	}
	if (0 != args.length % 2)
	    return false;

	Map<String, String> amap = new HashMap<String,String>();
	for (int i = 0; i < args.length; i+=2) {
	    amap.put(args[i], args[i+1]);
	}

	host = amap.remove("-host");
	if (null == host) { // host is required
	    System.err.println("Must provide a host");
	    return false;
	}

	filename = amap.remove("-f");
	if (null == filename) { // filename is required
	    System.err.println("Must provide an output filename stem");
	    return false;
	}

	cqlSchema = amap.remove("-schema");
	if (null == cqlSchema) { // schema is required
	    System.err.println("Must provide a schema");
	    return false;
	}

	String tkey;
	if (null != (tkey = amap.remove("-port")))          port = Integer.parseInt(tkey);
	if (null != (tkey = amap.remove("-user")))          username = tkey;
	if (null != (tkey = amap.remove("-pw")))            password = tkey;
	if (null != (tkey = amap.remove("-dateFormat")))    dateFormatString = tkey;
	if (null != (tkey = amap.remove("-nullString")))    nullString = tkey;
	if (null != (tkey = amap.remove("-delim")))         delimiter = tkey;
	if (null != (tkey = amap.remove("-decimalDelim"))) {
	    if (tkey.equals(","))
		locale = Locale.FRANCE;
	}
	if (null != (tkey = amap.remove("-boolStyle"))) {
	    boolStyle = BooleanParser.getBoolStyle(tkey);
	    if (null == boolStyle) {
		System.err.println("Bad boolean style.  Options are: " + BooleanParser.getOptions());
		return false;
	    }
	}
	if (null != (tkey = amap.remove("-numThreads")))    numThreads = Integer.parseInt(tkey);
	if (null != (tkey = amap.remove("-beginToken")))    beginToken = tkey;
	if (null != (tkey = amap.remove("-endToken")))      endToken = tkey;
	
	if (!amap.isEmpty()) {
	    for (String k : amap.keySet())
		System.err.println("Unrecognized option: " + k);
	    return false;
	}
	return validateArgs();
    }

    private void setup() {
	// Connect to Cassandra
	Cluster.Builder clusterBuilder = Cluster.builder()
	    .addContactPoint(host)
	    .withPort(port)
	    .withLoadBalancingPolicy(new TokenAwarePolicy( new DCAwareRoundRobinPolicy()));
	if (null != username)
	    clusterBuilder = clusterBuilder.withCredentials(username, password);
	cluster = clusterBuilder.build();
	session = cluster.newSession();
    }

    private void cleanup() {
	if (null != session)
	    session.close();
	if (null != cluster)
	    cluster.close();
    }
    
    public boolean run(String[] args) throws IOException, ParseException, 
					     InterruptedException, 
					     ExecutionException {
	if (false == parseArgs(args)) {
	    System.err.println("Bad arguments");
	    System.err.println(usage());
	    return false;
	}

	// Setup
	setup();

	PrintStream pstream = null;
	if (1 == numThreads) {
	    if (filename.equalsIgnoreCase("stdout")) {
		pstream = System.out;
	    }
	    else {
		pstream = new PrintStream(new BufferedOutputStream(new FileOutputStream(filename + ".0")));
	    }
	}
	
	// Launch Threads
	ExecutorService executor;
	long total = 0;
	if (null != pstream) {
	    // One file/stdin to process
	    executor = Executors.newSingleThreadExecutor();
	    Callable<Long> worker = new ThreadExecute(cqlSchema, delimiter, 
						      nullString,
						      delimiterInQuotes, 
						      dateFormatString, 
						      boolStyle, locale, 
						      pstream, 
						      beginToken,
						      endToken, session);
	    Future<Long> res = executor.submit(worker);
	    total = res.get();
	    executor.shutdown();
	}
	else {
	    BigInteger begin = null;
	    BigInteger end = null;
	    BigInteger delta = null;
	    List<String> beginList = new ArrayList<String>();
	    List<String> endList = new ArrayList<String>();
	    if (null != beginToken) {
		begin = new BigInteger(beginToken);
		end = new BigInteger(endToken);
		delta = end.subtract(begin).divide(new BigInteger(String.valueOf(numThreads)));
		for (int mype = 0; mype < numThreads; mype++) {
		    if (mype < numThreads - 1) {
			beginList.add(begin.add(delta.multiply(new BigInteger(String.valueOf(mype)))).toString());
			endList.add(begin.add(delta.multiply(new BigInteger(String.valueOf(mype+1)))).toString());
		    }
		    else {
			beginList.add(begin.add(delta.multiply(new BigInteger(String.valueOf(numThreads-1)))).toString());
			endList.add(end.toString());
		    }
		}
	    }
	    else {
		// What's the right thing here?
		// (1) Split into canonical token ranges - numThreads=numRanges
		// (2) Split into subranges of canonical token ranges
		//     - if numThreads < numRanges, then reset numThreads=numRanges
		//     - let K=CEIL(numThreads/numRanges) and M=MOD(numThreads/numRanges), for the first M token ranges split into K subranges, and for the remaining ones split into K-1 subranges
		// (?) Should there be an option for numThreads-per-range?
		// (?) Should there be an option for numThreads=numRanges
	    }

	    executor = Executors.newFixedThreadPool(numThreads);
	    Set<Future<Long>> results = new HashSet<Future<Long>>();
	    for (int mype = 0; mype < numThreads; mype++) {
		String tBeginString = beginList.get(mype);
		String tEndString = endList.get(mype);
		pstream = new PrintStream(new BufferedOutputStream(new FileOutputStream(filename + "." + mype)));
		Callable<Long> worker = new ThreadExecute(cqlSchema, delimiter, 
							  nullString,
							  delimiterInQuotes, 
							  dateFormatString, 
							  boolStyle, locale, 
							  pstream, 
							  tBeginString,
							  tEndString, session);
		results.add(executor.submit(worker));
	    }
	    executor.shutdown();
	    for (Future<Long> res : results)
		total += res.get();
	}
	System.err.println("Total rows retrieved: " + total);

	// Cleanup
	cleanup();

	return true;
    }

    public static void main(String[] args) throws IOException, ParseException, InterruptedException, ExecutionException {
	CqlDelimUnload cdu = new CqlDelimUnload();
	cdu.run(args);
    }

    class ThreadExecute implements Callable<Long> {
	private Session session;
	private PreparedStatement statement;
	private CqlDelimParser cdp;

	private String cqlSchema;
	private Locale locale = null;
	private BooleanParser.BoolStyle boolStyle = null;
	private String nullString = null;
	private String delimiter = null;
	private boolean delimiterInQuotes;

	private PrintStream writer = null;
	private String beginToken = null;
	private String endToken = null;
	private String partitionKey = null;
	private long numRead = 0;

	public ThreadExecute(String inCqlSchema, String inDelimiter, 
			     String inNullString, boolean inDelimiterInQuotes, 
			     String inDateFormatString,
			     BooleanParser.BoolStyle inBoolStyle, 
			     Locale inLocale, 
			     PrintStream inWriter,
			     String inBeginToken, String inEndToken,
			     Session inSession) {
	    super();
	    cqlSchema = inCqlSchema;
	    delimiter = inDelimiter;
	    nullString = inNullString;
	    delimiterInQuotes = inDelimiterInQuotes;
	    dateFormatString = inDateFormatString;
	    boolStyle = inBoolStyle;
	    locale = inLocale;
	    beginToken = inBeginToken;
	    endToken = inEndToken;
	    session = inSession;
	    writer = inWriter;
	}

	public Long call() throws IOException, ParseException {
	    setup();
	    numRead = execute();
	    cleanup();
	    return numRead;
	}

	private String getPartitionKey(CqlDelimParser cdp, Session tsession) {
	    String query = "SELECT column_name, component_index, type "
		+ "FROM system.schema_columns WHERE keyspace_name = '"
		+ cdp.getKeyspace() + "' AND columnfamily_name = '"
		+ cdp.getTable() + "'";
            List<Row> rows = tsession.execute(query).all();
	    if (rows.isEmpty()) {
		// error
	    }
	    
	    int numberOfPartitionKeys = 0;
            for (Row row : rows)
                if (row.getString(2).equals("partition_key"))
                    numberOfPartitionKeys++;
            if (0 == numberOfPartitionKeys) {
		// error
	    }

            String[] partitionKeyArray = new String[numberOfPartitionKeys];
            for (Row row : rows) {
                String type = row.getString(2);
                String column = row.getString(0);
                if (type.equals("partition_key")) {
                    int componentIndex = row.isNull(1) ? 0 : row.getInt(1);
                    partitionKeyArray[componentIndex] = column;
                }
            }
	    String partitionKey = partitionKeyArray[0];
	    for (int i = 1; i < partitionKeyArray.length; i++) {
		partitionKey = partitionKey + "," + partitionKeyArray[i];
	    }
	    return partitionKey;
	}

	private void setup() throws IOException, ParseException {
	    cdp = new CqlDelimParser(cqlSchema, delimiter, nullString, 
				     delimiterInQuotes, dateFormatString, 
				     boolStyle, locale, session);
	    String select = cdp.generateSelect();
	    String partitionKey = getPartitionKey(cdp, session);
	    if (null != beginToken) {
		select = select + " WHERE Token(" + partitionKey + ") > " 
		    + beginToken + " AND Token(" + partitionKey + ") <= " 
		    + endToken;
	    }
	    statement = session.prepare(select);
	}
	
	private void cleanup() throws IOException {
	    writer.flush();
	    writer.close();
	}

	private long execute() throws IOException {
	    BoundStatement bound = statement.bind();
	    ResultSet rs = session.execute(bound);
	    numRead = 0;
	    for (Row row : rs) {
		writer.println(cdp.format(row));
		numRead++;
	    }
	    return numRead;
	}
    }
}

