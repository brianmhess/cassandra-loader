package hess.loader;

import hess.loader.parser.BooleanParser;
import hess.loader.parser.CqlDelimParser;

import java.lang.System;
import java.lang.String;
import java.lang.Integer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Locale;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.text.ParseException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;


public class CqlDelimLoad {
    private String host = null;
    private int port = 9042;
    private Cluster cluster = null;
    private Session session = null;
    private int numFutures = 1000;

    private String cqlSchema = null;

    private long maxErrors = 10;
    private long skipRows = 0;
    private long maxRows = -1;
    private String badFile = null;
    private BufferedWriter badWriter = null;
    private String filename = null;

    private Locale locale = null;
    private BooleanParser.BoolStyle boolStyle = null;
    private String dateFormatString = null;
    private String nullString = null;
    private String delimiter = null;
    private boolean delimiterInQuotes = false;

    private String usage() {
	String usage = "Usage: -f <filename> -host <ipaddress> -schema <schema> [OPTIONS]\n";
	usage = usage + "OPTIONS:\n";
	usage = usage + "  -delim <delimiter>             Delimiter to use [,]\n";
	usage = usage + "  -delmInQuotes true             Set to 'true' if delimiter can be inside quoted fields [false]\n";
	usage = usage + "  -dateFormat <dateFormatString> Date format [default for Locale.ENGLISH]\n";
	usage = usage + "  -nullString <nullString>       String that signifies NULL [none]\n";
	usage = usage + "  -skipRows <skipRows>           Number of rows to skip [0]\n";
	usage = usage + "  -maxRows <maxRows>             Maximum number of rows to read (-1 means all) [-1]\n";
	usage = usage + "  -maxErrors <maxErrors>         Maximum errors to endure [10]\n";
	usage = usage + "  -badFile <badFilename>         Filename for where to place badly parsed rows. [none]\n";
	usage = usage + "  -port <portNumber>             CQL Port Number [9042]\n";
	usage = usage + "  -numFutures <numFutures>       Number of CQL futures to keep in flight [1000]\n";
	usage = usage + "  -decimalDelim <decimalDelim>   Decimal delimiter [.] Other option is ','\n";
	usage = usage + "  -boolStyle <boolStyleString>   Style for booleans [TRUE_FALSE]\n";
	return usage;
    }
    
    private void setup() throws IOException {
	// Prepare Badfile
	if (null != badFile)
	    badWriter = new BufferedWriter(new FileWriter(badFile));

	// Connect to Cassandra
	cluster = Cluster.builder()
	    .addContactPoint(host)
	    .withPort(port)
	    .withLoadBalancingPolicy(new TokenAwarePolicy( new DCAwareRoundRobinPolicy()))
	    .build();
	session = cluster.newSession();
    }

    private void cleanup() throws IOException {
	if (null != badWriter)
	    badWriter.close();
	if (null != session)
	    session.close();
	if (null != cluster)
	    cluster.close();
    }

    private boolean validateArgs() {
	if (0 >= numFutures) {
	    System.err.println("Number of futures must be positive");
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
	    System.err.println("Maximum number of errors must be non-negative");
	    return false;
	}

	return true;
    }
    
    private boolean parseArgs(String[] args) {
	if ((args.length == 0) || (0 != args.length % 2))
	    return false;
	Map<String, String> amap = new HashMap<String,String>();
	for (int i = 0; i < args.length; i+=2)
	    amap.put(args[i], args[i+1]);

	host = amap.remove("-host");
	if (null == host) // host is required
	    return false;

	filename = amap.remove("-f");
	if (null == filename) // filename is required
	    return false;

	cqlSchema = amap.remove("-schema");
	if (null == cqlSchema) // schema is required
	    return false; 

	String tkey;
	if (null != (tkey = amap.remove("-port")))          port = Integer.parseInt(tkey);
	if (null != (tkey = amap.remove("-numFutures")))    numFutures = Integer.parseInt(tkey);
	if (null != (tkey = amap.remove("-maxErrors")))     maxErrors = Integer.parseInt(tkey);
	if (null != (tkey = amap.remove("-skipRows")))      skipRows = Integer.parseInt(tkey);
	if (null != (tkey = amap.remove("-maxRows")))       maxRows = Integer.parseInt(tkey);
	if (null != (tkey = amap.remove("-badFile")))       badFile = tkey;
	if (null != (tkey = amap.remove("-dateFormat")))    dateFormatString = tkey;
	if (null != (tkey = amap.remove("-nullString")))    nullString = tkey;
	if (null != (tkey = amap.remove("-delim")))         delimiter = tkey;
	if (null != (tkey = amap.remove("-delimInQuotes"))) delimiterInQuotes = Boolean.parseBoolean(tkey);
	if (null != (tkey = amap.remove("-decimalDelim"))) {
	    if (tkey.equals(","))
		locale = Locale.FRANCE;
	}
	if (null != (tkey = amap.remove("-boolStyle"))) {
	    BooleanParser.BoolStyle bs = BooleanParser.getBoolStyle(tkey);
	    if (null == bs) {
		System.err.println("Bad boolean style.  Options are: " + BooleanParser.getOptions());
		return false;
	    }
	}
	if (-1 == maxRows)
	    maxRows = Long.MAX_VALUE;
	if (-1 == maxErrors)
	    maxErrors = Long.MAX_VALUE;
	
	if (!amap.isEmpty()) {
	    for (String k : amap.keySet())
		System.err.println("Unrecognized option: " + k);
	    return false;
	}
	return validateArgs();
    }

    private void purgeFutures(List<ResultSetFuture> futures) {
	for (ResultSetFuture future: futures) {
	    future.getUninterruptibly();
	}
	futures.clear();
    }

    public boolean run(String[] args) throws IOException, ParseException {
	if (false == parseArgs(args)) {
	    System.err.println("Bad arguments");
	    System.err.println(usage());
	    return false;
	}
	
	// open file
	BufferedReader reader = null;
	if (filename.equalsIgnoreCase("stdin"))
	    reader = new BufferedReader(new InputStreamReader(System.in));
	else
	    reader = new BufferedReader(new FileReader(filename));

	// setup CQL stuff - parser, connection
	CqlDelimParser cdp = new CqlDelimParser(cqlSchema, delimiter, nullString, delimiterInQuotes, 
						dateFormatString, boolStyle, locale);
	String insert = cdp.generateInsert();
	setup();
	PreparedStatement statement = session.prepare(insert);
	List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
	String line;
	int lineNumber = 0;
	int numInserted = 0;
	int numErrors = 0;
	List<Object> elements;

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

	    if (0 == lineNumber % numFutures)
		purgeFutures(futures);

	    if (null != (elements = cdp.parse(line))) {
		BoundStatement bind = statement.bind(elements.toArray());
		ResultSetFuture resultSetFuture = session.executeAsync(bind);
		futures.add(resultSetFuture);
		numInserted++;
	    }
	    else {
		System.err.println(String.format("Error parsing line %d in %s: %s", lineNumber, filename, line));
		if (null != badWriter) {
		    badWriter.write(line);
		    badWriter.newLine();
		}
		numErrors++;
		if (maxErrors <= numErrors) {
		    System.err.println(String.format("Maximum number of errors exceeded (%d)", numErrors));
		    cluster.close();
		    return false;
		}
	    }
	}
	purgeFutures(futures);

	System.err.println("*** DONE: " + filename + "  number of lines processed: " + lineNumber + " (" + numInserted + " inserted)");

	cleanup();
	return true;
    }

    public static void main(String[] args) throws IOException, ParseException {
	CqlDelimLoad cdl = new CqlDelimLoad();
	cdl.run(args);
    }
}

