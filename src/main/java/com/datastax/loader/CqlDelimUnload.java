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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.math.BigInteger;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.KeyStoreException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.exceptions.QueryValidationException;


public class CqlDelimUnload {
    private String version = "0.0.27";
    private String host = null;
    private int port = 9042;
    private String username = null;
    private String password = null;
    private String truststorePath = null;
    private String truststorePwd = null;
    private String keystorePath = null;
    private String keystorePwd = null;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_ONE;
    private Cluster cluster = null;
    private Session session = null;
    private String beginToken = "-9223372036854775808";
    private String endToken = "9223372036854775807";
    private String where = null;

    private String cqlSchema = null;
    private String filename = null;
    private String format = "delim";

    private Locale locale = null;
    private BooleanParser.BoolStyle boolStyle = null;
    private String dateFormatString = null;
    private String localDateFormatString = "yyyy-MM-dd";
    private String nullString = null;
    private String delimiter = null;

    private int numThreads = 5;
    private int fetchSize = 0;

    private String usage() {
        StringBuilder usage = new StringBuilder("version: ").append(version).append("\n");
        usage.append("Usage: -f <outputStem> -host <ipaddress> -schema <schema> [OPTIONS]\n");
        usage.append("OPTIONS:\n");
        usage.append("  -configFile <filename>             File with configuration options\n");
        usage.append("  -format [delim|jsonline|jsonarray] Format of data: delimited or JSON [delim]\n");
        usage.append("  -delim <delimiter>                 Delimiter to use [,]\n");
        usage.append("  -dateFormat <dateFormatString>     Date format for TIMESTAMP [default for Locale.ENGLISH]\n");
        usage.append("  -localDateFormat <FormatString>    Date format for DATE [yyyy-MM-dd]\n");
        usage.append("  -nullString <nullString>           String that signifies NULL [none]\n");
        usage.append("  -port <portNumber>                 CQL Port Number [9042]\n");
        usage.append("  -user <username>                   Cassandra username [none]\n");
        usage.append("  -pw <password>                     Password for user [none]\n");
        usage.append("  -ssl-truststore-path <path>        Path to SSL truststore [none]\n");
        usage.append("  -ssl-truststore-pw <pwd>           Password for SSL truststore [none]\n");
        usage.append("  -ssl-keystore-path <path>          Path to SSL keystore [none]\n");
        usage.append("  -ssl-keystore-pw <pwd>             Password for SSL keystore [none]\n");
        usage.append("  -consistencyLevel <CL>             Consistency level [LOCAL_ONE]\n");
        usage.append("  -decimalDelim <decimalDelim>       Decimal delimiter [.] Other option is ','\n");
        usage.append("  -boolStyle <boolStyleString>       Style for booleans [TRUE_FALSE]\n");
        usage.append("  -numThreads <numThreads>           Number of concurrent threads to unload [5]\n");
        usage.append("  -beginToken <tokenString>          Begin token [none]\n");
        usage.append("  -endToken <tokenString>            End token [none]\n");
        usage.append("  -where <predicate>                 WHERE clause [none]\n");
        usage.append("  -fetchSize <fetchSize>             Fetch size to use [0]\n");
        return usage.toString();
    }
    
    private boolean validateArgs() {
        if (!format.equalsIgnoreCase("delim")
            && !format.equalsIgnoreCase("jsonline")
            && !format.equalsIgnoreCase("jsonarray")) {
            System.err.println("Invalid format (" + format + ")");
            return false;
        }
        if (numThreads < 1) {
            System.err.println("Number of threads must be positive");
            return false;
        }
        if (fetchSize < 0) {
            System.err.println("Fetch size must be non-negative");
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
        if ((null == truststorePath) && (null != truststorePwd)) {
            System.err.println("If you supply the ssl-truststore-pw, you must supply the ssl-truststore-path");
            return false;
        }
        if ((null != truststorePath) && (null == truststorePwd)) {
            System.err.println("If you supply the ssl-truststore-path, you must supply the ssl-truststore-pw");
            return false;
        }
        if ((null == keystorePath) && (null != keystorePwd)) {
            System.err.println("If you supply the ssl-keystore-pw, you must supply the ssl-keystore-path");
            return false;
        }
        if ((null != keystorePath) && (null == keystorePwd)) {
            System.err.println("If you supply the ssl-keystore-path, you must supply the ssl-keystore-pw");
            return false;
        }
        File tfile = null;
        if (null != truststorePath) {
            tfile = new File(truststorePath);
            if (!tfile.isFile()) {
                System.err.println("truststore file must be a file");
                return false;
            }
        }
        if (null != keystorePath) {
            tfile = new File(keystorePath);
            if (!tfile.isFile()) {
                System.err.println("keystore file must be a file");
                return false;
            }
        }
        if ((null != beginToken) && (null == endToken)) {
            System.err.println("If you supply the beginToken then you need to specify the endToken");
            return false;
        }
        if ((null == beginToken) && (null != endToken)) {
            System.err.println("If you supply the endToken then you need to specify the beginToken");
            return false;
        }

        return true;
    }
    
    private boolean processConfigFile(String fname, Map<String, String> amap)
        throws IOException, FileNotFoundException {
        File cFile = new File(fname);
        if (!cFile.isFile()) {
            System.err.println("Configuration File must be a file");
            return false;
        }

        BufferedReader cReader = new BufferedReader(new FileReader(cFile));
        String line;
        while ((line = cReader.readLine()) != null) {
            String[] fields = line.trim().split("\\s+");
            if (2 != fields.length) {
                System.err.println("Bad line in config file: " + line);
                return false;
            }
            if (null == amap.get(fields[0])) {
                amap.put(fields[0], fields[1]);
            }
        }
        return true;
    }

    private boolean parseArgs(String[] args)
        throws IOException, FileNotFoundException {
        String tkey;
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

        if (null != (tkey = amap.remove("-configFile")))
            if (!processConfigFile(tkey, amap))
                return false;

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

        if (null != (tkey = amap.remove("-port")))          port = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-user")))          username = tkey;
        if (null != (tkey = amap.remove("-pw")))            password = tkey;
        if (null != (tkey = amap.remove("-ssl-truststore-path"))) truststorePath = tkey;
        if (null != (tkey = amap.remove("-ssl-truststore-pw")))  truststorePwd =  tkey;
        if (null != (tkey = amap.remove("-ssl-keystore-path")))   keystorePath = tkey;
        if (null != (tkey = amap.remove("-ssl-keystore-pw")))    keystorePwd = tkey;
        if (null != (tkey = amap.remove("-consistencyLevel"))) consistencyLevel = ConsistencyLevel.valueOf(tkey);
        if (null != (tkey = amap.remove("-dateFormat")))    dateFormatString = tkey;
        if (null != (tkey = amap.remove("-localDateFormat")))    localDateFormatString = tkey;
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
        if (null != (tkey = amap.remove("-where")))         where = tkey;
        if (null != (tkey = amap.remove("-format")))        format = tkey;
        if (null != (tkey = amap.remove("-fetchSize")))     fetchSize = Integer.parseInt(tkey);
        
        if (!amap.isEmpty()) {
            for (String k : amap.keySet())
                System.err.println("Unrecognized option: " + k);
            return false;
        }
        return validateArgs();
    }

    private SSLOptions createSSLOptions()
        throws KeyStoreException, FileNotFoundException, IOException, NoSuchAlgorithmException,
               KeyManagementException, CertificateException, UnrecoverableKeyException {
        TrustManagerFactory tmf = null;
        KeyStore tks = KeyStore.getInstance("JKS");
        tks.load((InputStream) new FileInputStream(new File(truststorePath)),
                 truststorePwd.toCharArray());
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(tks);

        KeyManagerFactory kmf = null;
        if (null != keystorePath) {
            KeyStore kks = KeyStore.getInstance("JKS");
            kks.load((InputStream) new FileInputStream(new File(keystorePath)),
                     keystorePwd.toCharArray());
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(kks, keystorePwd.toCharArray());
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf != null? kmf.getKeyManagers() : null,
                        tmf != null ? tmf.getTrustManagers() : null,
                        new SecureRandom());

        return RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();
    }

    private void setup()
        throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
               CertificateException, UnrecoverableKeyException  {
        // Connect to Cassandra
        PoolingOptions pOpts = new PoolingOptions();
        pOpts.setCoreConnectionsPerHost(HostDistance.LOCAL, 4);
        pOpts.setMaxConnectionsPerHost(HostDistance.LOCAL, 4);
        Cluster.Builder clusterBuilder = Cluster.builder()
            .addContactPoint(host)
            .withPort(port)
            .withPoolingOptions(pOpts)
            .withLoadBalancingPolicy(new TokenAwarePolicy( DCAwareRoundRobinPolicy.builder().build()));
        if (null != username)
            clusterBuilder = clusterBuilder.withCredentials(username, password);
        if (null != truststorePath)
            clusterBuilder = clusterBuilder.withSSL(createSSLOptions());

        cluster = clusterBuilder.build();
        if (null == cluster) {
            throw new IOException("Could not create cluster");
        }
        session = cluster.connect();
    }

    private void cleanup() {
        if (null != session)
            session.close();
        if (null != cluster)
            cluster.close();
    }
    
    public boolean run(String[] args) 
        throws IOException, ParseException, InterruptedException, ExecutionException,
               KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
               CertificateException, UnrecoverableKeyException {
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
            beginToken = null;
            endToken = null;
        }
        
        // Launch Threads
        ExecutorService executor;
        long total = 0;
        if (null != pstream) {
            // One file/stdin to process
            executor = Executors.newSingleThreadExecutor();
            Callable<Long> worker = new ThreadExecute(cqlSchema, delimiter, 
                                                      nullString,
                                                      dateFormatString, 
                                                      localDateFormatString, 
                                                      boolStyle, locale, 
                                                      pstream, 
                                                      beginToken,
                                                      endToken, session,
                                                      consistencyLevel, where,
                                                      format, fetchSize);
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
            try{
                executor = Executors.newFixedThreadPool(numThreads);
                Set<Future<Long>> results = new HashSet<Future<Long>>();
                for (int mype = 0; mype < numThreads; mype++) {
                    String tBeginString = beginList.get(mype);
                    String tEndString = endList.get(mype);
                    final BufferedOutputStream bufferedOutputStream= new BufferedOutputStream(new FileOutputStream(filename + "." + mype));
                    pstream = new PrintStream(bufferedOutputStream);
                    Callable<Long> worker = new ThreadExecute(cqlSchema, delimiter,
                        nullString,
                        dateFormatString,
                        localDateFormatString,
                        boolStyle, locale,
                        pstream,
                        tBeginString,
                        tEndString, session,
                        consistencyLevel,
                        where, format, fetchSize);
                    results.add(executor.submit(worker));
                }
                executor.shutdown();
                for (Future<Long> res : results)
                    total += res.get();
            }catch (final Exception e){
                System.err.println(e.getMessage());
                System.exit(1);
            }

        }
        System.err.println("Total rows retrieved: " + total);

        // Cleanup
        cleanup();

        return true;
    }

    public static void main(String[] args) 
        throws IOException, ParseException, InterruptedException, ExecutionException,
               KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException,
               CertificateException, KeyManagementException  {
        CqlDelimUnload cdu = new CqlDelimUnload();
        boolean success = cdu.run(args);
        if (success) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }

    class ThreadExecute implements Callable<Long> {
        private Session session;
        private ConsistencyLevel consistencyLevel;
        private PreparedStatement statement;
        private CqlDelimParser cdp;

        private String cqlSchema;
        private String format = "delim";
        private Locale locale = null;
        private BooleanParser.BoolStyle boolStyle = null;
        private String nullString = null;
        private String delimiter = null;

        private PrintStream writer = null;
        private String beginToken = null;
        private String endToken = null;
        private String partitionKey = null;
        private long numRead = 0;
        private String where = null;
        private String dateFormatString = null;
        private String localDateFormatString = null;
        private int fetchSize = 0;

        public ThreadExecute(String inCqlSchema, String inDelimiter, 
                             String inNullString, 
                             String inDateFormatString,
                             String inLocalDateFormatString,
                             BooleanParser.BoolStyle inBoolStyle, 
                             Locale inLocale, 
                             PrintStream inWriter,
                             String inBeginToken, String inEndToken,
                             Session inSession, ConsistencyLevel inConsistencyLevel,
                             String inWhere, String inFormat, int inFetchSize) {
            super();
            cqlSchema = inCqlSchema;
            delimiter = inDelimiter;
            nullString = inNullString;
            dateFormatString = inDateFormatString;
            localDateFormatString = inLocalDateFormatString;
            boolStyle = inBoolStyle;
            locale = inLocale;
            beginToken = inBeginToken;
            endToken = inEndToken;
            session = inSession;
            writer = inWriter;
            consistencyLevel = inConsistencyLevel;
            where = inWhere;
            format = inFormat;
            fetchSize = inFetchSize;
        }

        public Long call() throws IOException, ParseException {
            if (false == setup()) {
                return 0L;
            }
            numRead = execute();
            cleanup();
            return numRead;
        }

        private String getPartitionKey(CqlDelimParser cdp, Session session) {
            String keyspace = cdp.getKeyspace();
            String table = cdp.getTable();
            if (keyspace.startsWith("\"") && keyspace.endsWith("\""))
                keyspace = keyspace.replaceAll("\"", "");
            else
                keyspace = keyspace.toLowerCase();
            if (table.startsWith("\"") && table.endsWith("\""))
                table = table.replaceAll("\"", "");
            else
                table = table.toLowerCase();

            List<ColumnMetadata> lcm = session.getCluster().getMetadata()
                .getKeyspace(keyspace).getTable(table).getPartitionKey();
            String partitionKey = lcm.get(0).getName();
            for (int i = 1; i < lcm.size(); i++) {
                partitionKey = partitionKey + "," + lcm.get(i).getName();
            }
            return partitionKey;
        }

        private boolean setup() throws IOException, ParseException {
            cdp = new CqlDelimParser(cqlSchema, delimiter, 4096, nullString, 
                                     null, dateFormatString, localDateFormatString,
                                     boolStyle, locale, null, session, false, -1);
            String select = cdp.generateSelect();
            String partitionKey = getPartitionKey(cdp, session);
            if (null != beginToken) {
                select = select + " WHERE Token(" + partitionKey + ") > " 
                    + beginToken + " AND Token(" + partitionKey + ") <= " 
                    + endToken;
                if (null != where)
                    select = select + " AND " + where;
            }
            else {
                if (null != where)
                    select = select + " WHERE " + where;
            }
            try {
                statement = session.prepare(select);
            }
            catch (QueryValidationException iqe) {
                System.err.println("Error creating statement: " + iqe.getMessage());
                System.err.println("CQL Query: " + select);
                if (null != where)
                    System.err.println("Check your syntax for -where: " + where);
                return false;
            }
            statement.setConsistencyLevel(consistencyLevel);
            return true;
        }
        
        private void cleanup() throws IOException {
            writer.flush();
            writer.close();
        }

        private long execute() throws IOException {
            BoundStatement bound = statement.bind();
            bound.setFetchSize(fetchSize);            
            ResultSet rs = session.execute(bound);
            numRead = 0;
            String s = null;
            String jsonArrayChar = "[\n";
            for (Row row : rs) {
                if (format.equalsIgnoreCase("jsonarray")) {
                    writer.print(jsonArrayChar);
                    jsonArrayChar = ",";
                }
                if (format.equalsIgnoreCase("delim")) {
                    s = cdp.format(row);
                }
                else if (format.equalsIgnoreCase("jsonline")
                         || format.equalsIgnoreCase("jsonarray")) {
                    s = cdp.formatJson(row);
                }
                writer.println(s);
                numRead++;
            }
            if (format.equalsIgnoreCase("jsonarray"))
                writer.println("]");
            return numRead;
        }
    }
}

