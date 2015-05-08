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
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.loader.futures.FutureManager;
import com.datastax.loader.futures.PrintingFutureSet;
import com.datastax.loader.parser.BooleanParser;
import com.google.common.util.concurrent.RateLimiter;

public class CqlDelimLoad {
	private String version = "0.0.9";
	private String host = null;
	private int port = 9042;
	private String username = null;
	private String password = null;
	private Cluster cluster = null;
	private Session session = null;
	private int numFutures = 1000;
	private int inNumFutures = -1;
	private int queryTimeout = 2;
	private long maxInsertErrors = 10;
	private int numRetries = 1;
	private double rate = 50000.0;
	private RateLimiter rateLimiter = null;
	
	private long loadStartTime= System.currentTimeMillis();

	private String cqlSchema = null;

	private long maxErrors = 10;
	private long skipRows = 0;
	private long maxRows = -1;
	private String badDir = ".";
	private String filename = null;
	public static String STDIN = "stdin";
	private String successDir = null;
	private String failureDir = null;

	private Locale locale = null;
	private BooleanParser.BoolStyle boolStyle = null;
	private String dateFormatString = null;
	private String nullString = null;
	private String delimiter = null;
	private boolean delimiterInQuotes = false;

	private int numThreads = Runtime.getRuntime().availableProcessors();

	
	protected static final AtomicLong totalRecordsWritten = new AtomicLong(0);
	
	private String usage() {
		StringBuilder usage = new StringBuilder("version: ").append(version).append("\n");
		usage.append("Usage: -f <filename> -host <ipaddress> -schema <schema> [OPTIONS]\n");
		usage.append("OPTIONS:\n");
		usage.append("  -delim <delimiter>             Delimiter to use [,]\n");
		usage.append("  -delimInQuotes true            Set to 'true' if delimiter can be inside quoted fields [false]\n");
		usage.append("  -dateFormat <dateFormatString> Date format [default for Locale.ENGLISH]\n");
		usage.append("  -nullString <nullString>       String that signifies NULL [none]\n");
		usage.append("  -skipRows <skipRows>           Number of rows to skip [0]\n");
		usage.append("  -maxRows <maxRows>             Maximum number of rows to read (-1 means all) [-1]\n");
		usage.append("  -maxErrors <maxErrors>         Maximum parse errors to endure [10]\n");
		usage.append("  -badDir <badDirectory>         Directory for where to place badly parsed rows. [none]\n");
		usage.append("  -port <portNumber>             CQL Port Number [9042]\n");
		usage.append("  -user <username>               Cassandra username [none]\n");
		usage.append("  -pw <password>                 Password for user [none]\n");
		usage.append("  -numFutures <numFutures>       Number of CQL futures to keep in flight [1000]\n");
		usage.append("  -decimalDelim <decimalDelim>   Decimal delimiter [.] Other option is ','\n");
		usage.append("  -boolStyle <boolStyleString>   Style for booleans [TRUE_FALSE]\n");
		usage.append("  -numThreads <numThreads>       Number of concurrent threads (files) to load [num cores]\n");
		usage.append("  -queryTimeout <# seconds>      Query timeout (in seconds) [2]\n");
		usage.append("  -numRetries <numRetries>       Number of times to retry the INSERT [1]\n");
		usage.append("  -maxInsertErrors <# errors>    Maximum INSERT errors to endure [10]\n");
		usage.append("  -rate <rows-per-second>        Maximum insert rate [50000]\n");
		usage.append("  -successDir <dir>              Directory where to move successfully loaded files\n");
		usage.append("  -failureDir <dir>              Directory where to move files that did not successfully load\n");

		usage.append("\n\nExamples:\n");
		usage.append("cassandra-loader -f /path/to/file.csv -host localhost -schema \"test.test3(a, b, c)\"\n");
		usage.append("cassandra-loader -f /path/to/directory -host 1.2.3.4 -schema \"test.test3(a, b, c)\" -delim \"\\t\" -numThreads 10\n");
		usage.append("cassandra-loader -f stdin -host localhost -schema \"test.test3(a, b, c)\" -user myuser -pw mypassword\n");
		
		return usage.toString();
	}

	private boolean validateArgs() {
		if (0 >= numFutures) {
			System.err.println("Number of futures must be positive (" + numFutures + ")");
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
		if (0 > numRetries) {
			System.err.println("Number of retries must be non-negative");
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
		if (numThreads < 1) {
			System.err.println("Number of threads must be non-negative");
			return false;
		}
		if (!STDIN.equalsIgnoreCase(filename)) {
			File infile = new File(filename);
			if ((!infile.isFile()) && (!infile.isDirectory())) {
				System.err.println("The -f argument needs to be a file or a directory");
				return false;
			}
			if (infile.isDirectory()) {
				File[] infileList = infile.listFiles();
				if (infileList.length < 1) {
					System.err.println("The directory supplied is empty");
					return false;
				}
			}
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
		if ((null == username) && (null != password)) {
			System.err.println("If you supply the password, you must supply the username");
			return false;
		}
		if ((null != username) && (null == password)) {
			System.err.println("If you supply the username, you must supply the password");
			return false;
		}

		if (0 > rate) {
			System.err.println("Rate must be positive");
			return false;
		}

		return true;
	}

	private boolean parseArgs(String[] args) {
		if (args.length == 0) {
			System.err.println("No arguments specified");
			return false;
		}
		if (0 != args.length % 2) {
			System.err.println("Not an even number of parameters");
			return false;
		}

		Map<String, String> amap = new HashMap<String,String>();
		for (int i = 0; i < args.length; i+=2)
			amap.put(args[i], args[i+1]);

		host = amap.remove("-host");
		if (null == host) { // host is required
			System.err.println("Must provide a host");
			return false;
		}

		filename = amap.remove("-f");
		if (null == filename) { // filename is required
			System.err.println("Must provide a filename/directory");
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
		if (null != (tkey = amap.remove("-numFutures")))    inNumFutures = Integer.parseInt(tkey);
		if (null != (tkey = amap.remove("-queryTimeout")))  queryTimeout = Integer.parseInt(tkey);
		if (null != (tkey = amap.remove("-maxInsertErrors"))) maxInsertErrors = Long.parseLong(tkey);
		if (null != (tkey = amap.remove("-numRetries")))    numRetries = Integer.parseInt(tkey);
		if (null != (tkey = amap.remove("-maxErrors")))     maxErrors = Long.parseLong(tkey);
		if (null != (tkey = amap.remove("-skipRows")))      skipRows = Integer.parseInt(tkey);
		if (null != (tkey = amap.remove("-maxRows")))       maxRows = Integer.parseInt(tkey);
		if (null != (tkey = amap.remove("-badDir")))        badDir = tkey;
		if (null != (tkey = amap.remove("-dateFormat")))    dateFormatString = tkey;
		if (null != (tkey = amap.remove("-nullString")))    nullString = tkey;
		if (null != (tkey = amap.remove("-delim")))         delimiter = tkey;
		if (null != (tkey = amap.remove("-delimInQuotes"))) delimiterInQuotes = Boolean.parseBoolean(tkey);
		if (null != (tkey = amap.remove("-numThreads")))    numThreads = Integer.parseInt(tkey);
		if (null != (tkey = amap.remove("-rate")))          rate = Double.parseDouble(tkey);
		if (null != (tkey = amap.remove("-successDir")))    successDir = tkey;
		if (null != (tkey = amap.remove("-failureDir")))    failureDir = tkey;
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
		if (-1 == maxRows)
			maxRows = Long.MAX_VALUE;
		if (-1 == maxErrors)
			maxErrors = Long.MAX_VALUE;
		if (-1 == maxInsertErrors)
			maxInsertErrors = Long.MAX_VALUE;

		if (!amap.isEmpty()) {
			for (String k : amap.keySet())
				System.err.println("Unrecognized option: " + k);
			return false;
		}

		if (0 < inNumFutures)
			numFutures = inNumFutures / numThreads;

		return validateArgs();
	}

	private void setup() {
		// Connect to Cassandra
		System.out.println("Number of Threads: "+ numThreads);
		
		Cluster.Builder clusterBuilder = Cluster.builder()
				.addContactPoint(host)
				.withPort(port)
				.withProtocolVersion(ProtocolVersion.V2) // Should be V3, but issues for now....
				.withLoadBalancingPolicy(new TokenAwarePolicy( new DCAwareRoundRobinPolicy(), true));
		if (null != username)
			clusterBuilder = clusterBuilder.withCredentials("mdp", "mdp-");	
		clusterBuilder = clusterBuilder.withCredentials("mdp", "mdp-");	
		cluster = clusterBuilder.build();
		session = cluster.newSession();
		if (0 < rate)
			rateLimiter = RateLimiter.create(rate);
			
	}

	private void cleanup() {
		if (null != session)
			session.close();
		if (null != cluster)
			cluster.close();
	}

	public boolean run(String[] args) throws IOException, ParseException, InterruptedException, ExecutionException {
		if (false == parseArgs(args)) {
			System.err.println("Bad arguments");
			System.err.println(usage());
			return false;
		}

		// Setup
		setup();

		// open file
		BufferedReader reader = null;
		String readerName = "";
		Deque<File> fileList = new ArrayDeque<File>();
		File infile = null;
		File[] inFileList = null;
		boolean onefile = true;
		if (STDIN.equalsIgnoreCase(filename)) {
			infile = null;
		}
		else {
			infile = new File(filename);
			if (infile.isFile()) {
			}
			else {
				inFileList = infile.listFiles();
				if (inFileList.length < 1)
					throw new IOException("directory is empty");
				onefile = false;
				Arrays.sort(inFileList, 
						new Comparator<File>() {
					public int compare(File f1, File f2) {
						return f1.getName().compareTo(f2.getName());
					}
				});
				for (int i = 0; i < inFileList.length; i++)
					fileList.push(inFileList[i]);
			}
		}

		// Launch Threads
		ExecutorService executor;
		long total = 0;
		if (onefile) {
			// One file/stdin to process
			executor = Executors.newSingleThreadExecutor();
			Callable<Long> worker = new ThreadExecute(cqlSchema, delimiter, 
					nullString,
					delimiterInQuotes, 
					dateFormatString, 
					boolStyle, locale, 
					maxErrors, skipRows,
					maxRows, badDir, infile, 
					session, numFutures,
					numRetries, queryTimeout,
					maxInsertErrors, 
					rateLimiter, 
					successDir, failureDir);
			Future<Long> res = executor.submit(worker);
			loadStartTime=System.currentTimeMillis();
			System.err.println("["+new Date()+"]: Datastax Loader for single file started " );
			total = res.get();
			executor.shutdown();
		}
		else {
			executor = Executors.newFixedThreadPool(numThreads);
			Set<Future<Long>> results = new HashSet<Future<Long>>();
			while (!fileList.isEmpty()) {
				File tFile = fileList.pop();
				Callable<Long> worker = new ThreadExecute(cqlSchema, delimiter,
						nullString,
						delimiterInQuotes, 
						dateFormatString, 
						boolStyle, locale, 
						maxErrors, skipRows,
						maxRows, badDir, tFile, 
						session,
						numFutures,
						numRetries, 
						queryTimeout,
						maxInsertErrors, 
						rateLimiter,
						successDir, failureDir);
				results.add(executor.submit(worker));
				
			}
			System.err.println("["+new Date()+"]: Datastax Loader for multiple files started " );
			loadStartTime=System.currentTimeMillis();
			executor.shutdown();
			for (Future<Long> res : results)
				total += res.get();
		}
		long loaderEndTime=System.currentTimeMillis();
		long loaderTime=loaderEndTime-loadStartTime;
		System.err.println("["+new Date()+"]: Total rows inserted: " + total + " in "+ loaderTime/1000 +"secs");

		// Cleanup
		cleanup();

		return true;
	}

	public static void main(String[] args) throws IOException, ParseException, InterruptedException, ExecutionException {
		CqlDelimLoad cdl = new CqlDelimLoad();
		boolean success = cdl.run(args);
		if (success) {
			System.exit(0);
		} else {
			System.exit(-1);
		}
	}

	class ThreadExecute implements Callable<Long> {
		private String BADPARSE = ".BADPARSE";
		private String BADINSERT = ".BADINSERT";
		private String LOG = ".LOG";
		private Session session;
		private String insert;
		private PreparedStatement statement;
		private CqlDelimParser cdp;
		private long maxErrors;
		private long skipRows;
		private long maxRows;
		private String badDir;
		private String successDir;
		private String failureDir;
		private String readerName;
		private PrintStream badParsePrinter = null;
		private PrintStream badInsertPrinter = null;
		private PrintStream logPrinter = null;
		private BufferedReader reader;
		private File infile;
		private int numFutures;
		private long numInserted;
		private final RateLimiter rateLimiter;

		private String cqlSchema;
		private Locale locale = null;
		private BooleanParser.BoolStyle boolStyle = null;
		private String nullString = null;
		private String delimiter = null;
		private boolean delimiterInQuotes;
		private TimeUnit unit = TimeUnit.SECONDS;
		private long queryTimeout = 2;
		private int numRetries = 1;
		private long maxInsertErrors = 10;
		private long insertErrors = 0;

		public ThreadExecute(String inCqlSchema, String inDelimiter, 
				String inNullString, boolean inDelimiterInQuotes, 
				String inDateFormatString,
				BooleanParser.BoolStyle inBoolStyle, 
				Locale inLocale, 
				long inMaxErrors, long inSkipRows, long inMaxRows,
				String inBadDir, File inFile,
				Session inSession,
				int inNumFutures, int inNumRetries, 
				int inQueryTimeout, long inMaxInsertErrors,
				RateLimiter inRateLimiter,
				String inSuccessDir, String inFailureDir) {
			super();
			cqlSchema = inCqlSchema;
			delimiter = inDelimiter;
			nullString = inNullString;
			delimiterInQuotes = inDelimiterInQuotes;
			dateFormatString = inDateFormatString;
			boolStyle = inBoolStyle;
			locale = inLocale;
			maxErrors = inMaxErrors;
			skipRows = inSkipRows;
			maxRows = inMaxRows;
			badDir = inBadDir;
			infile = inFile;
			session = inSession;
			numFutures = inNumFutures;
			numRetries = inNumRetries;
			queryTimeout = inQueryTimeout;
			maxInsertErrors = inMaxInsertErrors;
			rateLimiter = inRateLimiter;
			successDir = inSuccessDir;
			failureDir = inFailureDir;
		}

		public Long call() throws IOException, ParseException {
			setup();
			numInserted = execute();
			return numInserted;
		}

		private void setup() throws IOException, ParseException {
			if (null == infile) {
				reader = new BufferedReader(new InputStreamReader(System.in));
				//readerName = CqlDelimLoad.STDIN;
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
				logPrinter = new PrintStream(new BufferedOutputStream(new FileOutputStream(badDir + "/" + readerName + LOG)));
			}

			cdp = new CqlDelimParser(cqlSchema, delimiter, nullString, 
					delimiterInQuotes, dateFormatString, 
					boolStyle, locale, session);
			insert = cdp.generateInsert();
			statement = session.prepare(insert);
			statement.setRetryPolicy(new LoaderRetryPolicy(numRetries));
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

		private long execute() throws IOException {
			FutureManager fm = new PrintingFutureSet(numFutures, queryTimeout, maxInsertErrors, 
					logPrinter, badInsertPrinter);
			String line;
			int lineNumber = 0;
			long numInserted = 0;
			int numErrors = 0;
			List<Object> elements;

			System.err.println("["+new Date()+"]: "+"*** Processing " + readerName);

			Random rand = new Random();
						
			SimpleDateFormat sdf= new SimpleDateFormat("YYYY-MM-DD hh:mm:ss.SSS");
			long threadStartTime=System.currentTimeMillis();

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
					/*ArrayList list= new ArrayList<>();
					list.add(String.valueOf(rand.nextInt()));
					list.add(String.valueOf(rand.nextInt()));	
					list.add(String.valueOf(rand.nextInt()));	
					list.add(String.valueOf(rand.nextInt()));	
					list.add(String.valueOf(rand.nextInt()));				
					HashMap<String,String> map = new HashMap<String,String>();
					map.put("Name", "Srivatsan");				
					map.put("Age", String.valueOf(rand.nextInt()));
					map.put("Mail", "Srivatsan@");

					elements.add(list);
					elements.add(map);*/



					BoundStatement bind = statement.bind(elements.toArray());
					if (null != rateLimiter)
						rateLimiter.acquire(1);
					ResultSetFuture resultSetFuture = session.executeAsync(bind);
					if (!fm.add(resultSetFuture, line)) {
						cleanup(false);
						return -2;
					}
					
					long totwriten = totalRecordsWritten.incrementAndGet();
					numInserted++;
					if( totwriten%10000 == 0 )
					{					
						System.out.println("Inserted records :" + totalRecordsWritten.get() +" Time Taken: "+ (System.currentTimeMillis()-threadStartTime) + "ms");
						threadStartTime=System.currentTimeMillis();
					}
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
			if (!fm.cleanup()) {
				cleanup(false);
				return -1;
			}

			if (null != logPrinter) {
				logPrinter.println("*** DONE: " + readerName + "  number of lines processed: " + lineNumber + " (" + numInserted + " inserted)");
			}
			System.err.println("["+new Date()+"]: "+"*** DONE: " + readerName + "  number of lines processed: " + lineNumber + " (" + numInserted + " inserted)");

			cleanup(true);
			return fm.getNumInserted();
		}
	}
}

