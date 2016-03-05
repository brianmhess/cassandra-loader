# CASSANDRA-LOADER

## Introduction
cassandra-loader is a general-purpose, delimited-file, bulk loader for 
Cassandra. It supports a number of configuration options to enable bulk 
loading of various types of delimited files, including
* comma-separated values
* tab-separated values
* customer delimiter-separated values
* header row
* comma as decimal separator
* ...

## Getting it

### Downloading
This utility has already been built, and is available at
https://github.com/brianmhess/cassandra-loader/releases/download/v0.0.17/cassandra-loader

Get it with wget:
```
wget https://github.com/brianmhess/cassandra-loader/releases/download/v0.0.17/cassandra-loader
```

### Building
To build this repository, simply clone this repo and run:
```
gradle loader
```

All of the dependencies are included (namely, the Java driver - currently
version 2.1.4).  The output will be the cassandra-loader executable
in the build directory.  There will also be an jar with all of the
dependencies included in the build/libs/cassandra-loader-uber-<version>.jar

## Documentation 
To extract this README document, simply run (on the cassandra-loader
executable - (e.g., on build/cassandra-loader):
```
jar xf cassandra-loader README.md
```

## Run
To run cassandra-loader, simply run the cassandra-loader executable 
(e.g., located at build/cassandra-loader):
```
cassandra-loader
```
If you built this with gradle, you can also run:
```
gradle run
```

This will print the usage statment.

The following will load the myFileToLoad.csv file into the Cassandra 
cluster at IP address 1.2.3.4 into the test.ltest column family where 
the myFileToLoad file has the format of 4 columns - and it gets the
data type information from the database - and using the default options:
```
cassandra-loader -f myFileToLoad.csv -host 1.2.3.4 -schema "test.ltest(a, b, c, d)"
```

## Options:

 Switch           | Option             | Default                    | Description
-----------------:|-------------------:|---------------------------:|:----------
 `-configFile`    | Filename           | none                       | Filename of configuration options 
 `-f`             | Filename           | <REQUIRED>                 | Filename to load - required.
 `-host`          | IP Address         | <REQUIRED>                 | Cassandra connection point - required.
 `-schema`        | CQL schema         | <REQUIRED>                 | Schema of input data - required In the format "keySpace.table(col1,col2,...)" and in the order that the data will be in the file.
 `-port`          | Port Number        | 9042                       | Cassandra native protocol port number
 `-user`          | Username           | none                       | Cassandra username
 `-pw`            | Password           | none                       | Cassandra password
 `-ssl-truststore-path` | Truststore Path     | none                | Path to SSL truststore
 `-ssl-truststore-pwd`  | Truststore Password | none                | Password to SSL truststore
 `-ssl-keystore-path`   | Keystore Path       | none                | Path to SSL keystore
 `-ssl-keystore-path`   | Keystore Password   | none                | Password to SSL keystore
 '-consistencyLevel | Consistency Level | ONE                       | CQL Consistency Level
 `-numThreads`    | Number of threads  | Number of CPUs             | Number of threads to use (one per file)
 `-numFutures`    | Number of Futures  | 1000                       | Number of Java driver futures in flight.
 `-numRetries`    | Number of retries  | 1                          | Number of times to retry the INSERT before declaring defeat.
 `-queryTimeout`  | Timeout in seconds | 2                          | Amount of time to wait for a query to finish before timing out.
 `-delim`         | Delimiter          | ,                          | Delimiter to use
 `-nullString`    | Null String        | <empty string>             | String to represent NULL data
 `-boolStyle`     | Boolean Style      | TRUE_FALSE                 | String for boolean values.  Options are "1_0", "Y_N", "T_F", "YES_NO", "TRUE_FALSE".
 `-decimalDelim`  | Decimal delimiter  | .                          | Delimiter for decimal values.  Options are "." or ","
 `-dateFormat`    | Date Format String | default for Locale.ENGLISH | Date format string as specified in the SimpleDateFormat Java class: http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
 `-skipRows`      | Rows to skip       | 0                          | Number of rows to skip at the beginning of the file
 `-skipCols`      | Columns to skip    | <not set>                  | Comma-separated list of columns to skip loading (0-counted)
 `-maxRows`       | Max rows to read   | -1                         | Maximum rows to read (after optional skipping of rows).  -1 signifies all rows.
 `-maxErrors`     | Max parse errors   | 10                         | Maximum number of rows that do not parse to allow before exiting.
 `-maxInsertErrors`| Max insert errors | 10                         | Maximum number of rows that do not insert to allow before exiting.
 `-badDir`        | Bad directory      | current directory          | Directory to write badly parsed and badly inserted rows - as well as the log file.
 `-rate`          | Ingest rate        | unlimited                  | Maximum rate to insert data - in rows/sec.
 `-progressRate`  | Progress rate      | 100000                     | How often to report the ingest rate (number of rows)
 `-rateFile`      | Rate Stats File    | <not set>                  | File to contain CSV rate statistics
 `-successDir`    | Success directory  | <not set>                  | Location to move successfully loaded files
 `-failureDir`    | Failure directory  | <not set>                  | Location to move files that failed to load

## Comments
You can send data in on stdin by specifying the filename (via the -f switch) as "stdin" (case insensitive).
That way, you could pipe data in from other commands:
```
grep IMPORTANT data.csv | cassandra-loader -f stdin -h 1.2.3.4 -cql "test.itest(a, b)"
```

Collections are supported.  
Sets are started with '{' and ended with '}' and enclose a comma-separated list
{1,2,3} or {"a","b","c"}
Lists are started with '[' and ended with ']' and enclose a comma-separated list
[1,2,3] or ["a","b","c"]
Maps are started with '{' and ended with '}' and enclose a comma-separated list
of pairs that are separated by ':'
{1:1,2:2,3:3} or {"a":1, "b":2, "c":3}

If you specify either the username or the password, then you must specify both.

numFutures is a way to control the level of parallelism, but at some point 
too many will actually slow down the load.  The default of 500 is a decent 
place to start.

Double quotes will be stripped from all fields if they both begin and end
with ".

If you do not set the successDir then files that successfully loaded will remain in their input directory.  The same is true for failed files if you do not set the failureDir.  You cannot set either if the input file is "stdin".

boolStyle is a case-insensitive test of the True and False strings.  For the
different styles, the True and False strings are as follows:

```
    Style   | True | False
------------|------|-------
     0_1    |    1 |     0 
     Y_N    |    Y |     N 
     T_F    |    T |     F 
   YES_NO   |  YES |    NO 
 TRUE_FALSE | TRUE | FALSE 
```

configFile is a file with configuration options that are formatted just like on
the command line.  This allows you to not specify arguments on the command line.
For example, you can specify passwords in the configFile and avoid having them on
the command line.  The format is one switch and option per line:
```
-pwd mypassword
-host 1.2.3.4
```

### Case sensitivity

Keyspace, table, column identifiers are case insensitive in Apache Cassandra. Anyways the
`schema` parameter is treated internally as case sensitive and the queries used to load/unload
data are quoting all identifiers to preserve the case sensitivity.

## Usage Statement:
```
Usage: -f <filename> -host <ipaddress> -schema <schema> [OPTIONS]
OPTIONS:
  -configFile <filename>         File with configuration options
  -delim <delimiter>             Delimiter to use [,]
  -dateFormat <dateFormatString> Date format [default for Locale.ENGLISH]
  -nullString <nullString>       String that signifies NULL [none]
  -skipRows <skipRows>           Number of rows to skip [0]
  -skipCOls <columnsToSkip>      Comma-separated list of columsn to skip in the input file
  -maxRows <maxRows>             Maximum number of rows to read (-1 means all) [-1]
  -maxErrors <maxErrors>         Maximum parse errors to endure [10]
  -badDir <badDirectory>         Directory for where to place badly parsed rows. [none]
  -port <portNumber>             CQL Port Number [9042]
  -user <username>               Cassandra username [none]
  -pw <password>                 Password for user [none]
  -ssl-truststore-path <path>    Path to SSL truststore [none]
  -ssl-truststore-pw <pwd>       Password for SSL truststore [none]
  -ssl-keystore-path <path>      Path to SSL keystore [none]
  -ssl-keystore-pw <pwd>         Password for SSL keystore [none]
  -consistencyLevel <CL>         Consistency level [LOCAL_ONE]  -numFutures <numFutures>       Number of CQL futures to keep in flight [1000]
  -batchSize <batchSize>         Number of INSERTs to batch together [1]
  -decimalDelim <decimalDelim>   Decimal delimiter [.] Other option is ','
  -boolStyle <boolStyleString>   Style for booleans [TRUE_FALSE]
  -numThreads <numThreads>       Number of concurrent threads (files) to load [num cores]
  -queryTimeout <# seconds>      Query timeout (in seconds) [2]
  -numRetries <numRetries>       Number of times to retry the INSERT [1]
  -maxInsertErrors <# errors>    Maximum INSERT errors to endure [10]
  -rate <rows-per-second>        Maximum insert rate [50000]
  -progressRate <num txns>       How often to report the insert rate [100000]
  -rateFile <filename>           Where to print the rate statistics
  -successDir <dir>              Directory where to move successfully loaded files
  -failureDir <dir>              Directory where to move files that did not successfully load


Examples:
cassandra-loader -f /path/to/file.csv -host localhost -schema "test.test3(a, b, c)"
cassandra-loader -f /path/to/directory -host 1.2.3.4 -schema "test.test3(a, b, c)" -delim "\t" -numThreads 10
cassandra-loader -f stdin -host localhost -schema "test.test3(a, b, c)" -user myuser -pw mypassword
```

##Examples:
Load file /path/to/file.csv into the test3 table in the test keyspace using
the cluster at localhost.  Use the default options:
```
cassandra-loader -f /path/to/file.csv -host localhost -schema "test.test3(a, b, c)"
```
Load all the files from /path/to/directory into the test3 table in the test
keyspace using the cluster at 1.2.3.4.  Use 10 threads and use tab as the
delimiter:
```
cassandra-loader -f /path/to/directory -host 1.2.3.4 -schema "test.test3(a, b, c)" -delim "\t" -numThreads 10
```
Load the data from stdin into the test3 table in the test keyspace using the
cluster at localhost.  Use "myuser" as the username and "mypassword" as the
password:
```
cassandra-loader -f stdin -host localhost -schema "test.test3(a, b, c)" -user myuser -pw mypassword
```

##Sample
Included here is a set of sample data.  It is in the sample/ directory.
You can set up the table and keyspace by running:
```
cqlsh -f sample/cassandra-schema.cql
```

To load the data, run:
```
cd sample
./load.sh
```

To check that things have succeeded, you can run:
```
wc -l titanic.csv
```
And:
```
cqlsh -e "SELECT COUNT(*) FROM titanic.surviors"
```
Both should return 891.



## cassandra-unloader
cassandra-unloader is a utility to dump the contents
of a Cassandra table to delimited file format.  It uses
the same sorts of options as cassandra-loader so that the
output of cassandra-unloader could be piped into 
cassandra-loader:
```
cassandra-unloader -f stdout -host host1 -schema "ks.table(a,b,c)" | cassandra-loader -f stdin -host host2 -schema "ks2.table2(x,y,z)"
```

To build, run:
```
gradle unloader
```

To run cassandra-unloader, simply run the cassandra-unloader executable 
(e.g., located at build/cassandra-unloader):
```
cassandra-unloader
```

Usage statement:
```
version: 0.0.17
Usage: -f <outputStem> -host <ipaddress> -schema <schema> [OPTIONS]
OPTIONS:
  -configFile <filename>         File with configuration options
  -delim <delimiter>             Delimiter to use [,]
  -dateFormat <dateFormatString> Date format [default for Locale.ENGLISH]
  -nullString <nullString>       String that signifies NULL [none]
  -port <portNumber>             CQL Port Number [9042]
  -user <username>               Cassandra username [none]
  -pw <password>                 Password for user [none]
  -ssl-truststore-path <path>    Path to SSL truststore [none]
  -ssl-truststore-pw <pwd>       Password for SSL truststore [none]
  -ssl-keystore-path <path>      Path to SSL keystore [none]
  -ssl-keystore-pw <pwd>         Password for SSL keystore [none]
  -consistencyLevel <CL>         Consistency level [LOCAL_ONE]
  -decimalDelim <decimalDelim>   Decimal delimiter [.] Other option is ','
  -boolStyle <boolStyleString>   Style for booleans [TRUE_FALSE]
  -numThreads <numThreads>       Number of concurrent threads to unload [5]
  -beginToken <tokenString>      Begin token [none]
  -endToken <tokenString>        End token [none]
```
