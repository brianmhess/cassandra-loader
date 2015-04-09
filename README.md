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
https://github.com/brianmhess/cassandra-loader/releases/download/v0.0.3/cassandra-loader

Get it with wget:
```
https://github.com/brianmhess/cassandra-loader/releases/download/v0.0.3/cassandra-loader
```

### Building
To build this repository, simply clone this repo and run:
```
gradle buildit
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
the myFileToLoad file has the format of 4 columns - the first and 
second as integers, the third as text, and the fourth as 
double-precision floating point - and using the default options:
```
cassandra-loader -f myFileToLoad.csv -host 1.2.3.4 -schema "test.ltest(a int, b int, c text, d double)"
```

## Options:

 Switch           | Option             | Default                    | Description
-----------------:|-------------------:|---------------------------:|:----------
 `-f`             | Filename           | <REQUIRED>                 | Filename to load - required.
 `-host`          | IP Address         | <REQUIRED>                 | Cassandra connection point - required.
 `-schema`        | CQL schema         | <REQUIRED>                 | Schema of input data - required. Standard CQL schema (without PRIMARY KEY clause) and in the order that the data will be in the file.
 `-port`          | Port Number        | 9042                       | Cassandra native protocol port number
 `-user`          | Username           | none                       | Cassandra username
 `-pw`            | Password           | none                       | Cassandra password
 `-numFutures`    | Number of Futures  | 1000                       | Number of Java driver futures in flight.
 `-numRetries`    | Number of retries  | 1                          | Number of times to retry the INSERT before declaring defeat.
 `-numInserErrors` | Number of errors  | 10                         | Number of INSERT errors to tolerate before stopping.
 `-queryTimeout   | Timeout in seconds | 2                          | Amount of time to wait for a query to finish before timing out.
 `-delim`         | Delimiter          | ,                          | Delimiter to use
 `-delimInQuotes` | True/False         | false                      | Are delimiters allowed inside quoted strings? This is more expensive to parse, so we default to false.
 `-nullString`    | Null String        | <empty string>             | String to represent NULL data
 `-boolStyle`     | Boolean Style      | TRUE_FALSE                 | String for boolean values.  Options are "1_0", "Y_N", "T_F", "YES_NO", "TRUE_FALSE".
 `-decimalDelim`  | Decimal delimiter  | .                          | Delimiter for decimal values.  Options are "." or ","
 `-dateFormat`    | Date Format String | default for Locale.ENGLISH | Date format string as specified in the SimpleDateFormat Java class: http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
 `-skipRows`      | Rows to skip       | 0                          | Number of rows to skip at the beginning of the file
 `-maxRows`       | Max rows to read   | -1                         | Maximum rows to read (after optional skipping of rows).  -1 signifies all rows.
 `-maxErrors`     | Max parse errors   | 10                         | Maximum number of rows that do not parse to allow before exiting.
 `-badFile`       | Bad File           | <none>                     | File to write out badly parsed rows.

## Comments
You can send data in on stdin by specifying the filename (via the -f switch) as "stdin" (case insensitive).
That way, you could pipe data in from other commands:
```
grep IMPORTANT data.csv | cassandra-loader -f stdin -h 1.2.3.4 -cql "test.itest(a text, b text)"
```

If you specify either the username or the password, then you must specify both.

If you do not have delimiters inside quoted text fields, then leave the 
-delimInQuotes option false. Enabling it will result in slower parsing times.

numFutures is a way to control the level of parallelism, but at some point 
too many will actually slow down the load.  The default of 500 is a decent 
place to start.

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

## Usage Statement:
```
Usage: -f <filename> -host <ipaddress> -schema <schema> [OPTIONS]
OPTIONS:
  -delim <delimiter>             Delimiter to use [,]
  -delmInQuotes true             Set to 'true' if delimiter can be inside quoted fields [false]
  -dateFormat <dateFormatString> Date format [default for Locale.ENGLISH]
  -nullString <nullString>       String that signifies NULL [none]
  -skipRows <skipRows>           Number of rows to skip [0]
  -maxRows <maxRows>             Maximum number of rows to read (-1 means all) [-1]
  -maxErrors <maxErrors>         Maximum errors to endure [10]
  -badDir <badDirectory>         Directory for where to place badly parsed rows. [none]
  -port <portNumber>             CQL Port Number [9042]
  -user <username>               Cassandra username [none]
  -pw <password>                 Password for user [none]
  -numFutures <numFutures>       Number of CQL futures to keep in flight [1000]
  -decimalDelim <decimalDelim>   Decimal delimiter [.] Other option is ','
  -boolStyle <boolStyleString>   Style for booleans [TRUE_FALSE]
  -numThreads <numThreads>       Number of concurrent threads (files) to load [5]


Examples:
cassandra-loader -f /path/to/file.csv -host localhost -schema "test.test3(a int, b int, c int)"
cassandra-loader -f /path/to/directory -host 1.2.3.4 -schema "test.test3(a int, b int, c int)" -delim "\t" -numThreads 10
cassandra-loader -f stdin -host localhost -schema "test.test3(a int, b int, c int)" -user myuser -pw mypassword
```

##Examples:
Load file /path/to/file.csv into the test3 table in the test keyspace using
the cluster at localhost.  Use the default options:
```
cassandra-loader -f /path/to/file.csv -host localhost -schema "test.test3(a int, b int, c int)"
```
Load all the files from /path/to/directory into the test3 table in the test
keyspace using the cluster at 1.2.3.4.  Use 10 threads and use tab as the
delimiter:
```
cassandra-loader -f /path/to/directory -host 1.2.3.4 -schema "test.test3(a int, b int, c int)" -delim "\t" -numThreads 10
```
Load the data from stdin into the test3 table in the test keyspace using the
cluster at localhost.  Use "myuser" as the username and "mypassword" as the
password:
```
cassandra-loader -f stdin -host localhost -schema "test.test3(a int, b int, c int)" -user myuser -pw mypassword
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

