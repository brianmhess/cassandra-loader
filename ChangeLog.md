## 0.0.15
- Better error handling for case when C* inserts are failing

## 0.0.14
- Updated cassandra-unloader to add support for collections,
	consistency level, ssl, etc

## 0.0.13
- Added configFile
- added ssl options (with truststore and keystore)

## 0.0.12
- Added a rateFile to output CSV rate statistics
- added -skipCols to skip input columns

## 0.0.11
- Added support for quoted Keyspaces, Tables, and Columns

## 0.0.10
- You want collections?  You got 'em
- Added progress reporting - you can specify the rate at which
	the rate is reported via the -progressRate option
- Refactored RateLimiting - added it to a new RateLimitingSession
- Laid groundwork for Dynamic rate limiting - to be worked out
	once we find a way to collect the right statistic

## 0.0.9
- Added -successDir and -failureDir
- Added return codes for the loader and unloader
- Refactored BoolStyle
- Cleaned up the readme a bit

