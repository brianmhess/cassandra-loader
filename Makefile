dirs:
	- mkdir build

compile: dirs Parser NumberParser BigDecimalParser BigIntegerParser BooleanParser ByteBufferParser DateParser DoubleParser FloatParser InetAddressParser IntegerParser LongParser StringParser UUIDParser DelimParser CqlDelimParser CqlDelimLoad

Parser: src/main/java/com/datastax/loader/parser/Parser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/Parser.java

BigDecimalParser: src/main/java/com/datastax/loader/parser/BigDecimalParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/BigDecimalParser.java

BigIntegerParser: src/main/java/com/datastax/loader/parser/BigIntegerParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/BigIntegerParser.java

BooleanParser: src/main/java/com/datastax/loader/parser/BooleanParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/BooleanParser.java

ByteBufferParser: src/main/java/com/datastax/loader/parser/ByteBufferParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/ByteBufferParser.java

DateParser: src/main/java/com/datastax/loader/parser/DateParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/DateParser.java

DoubleParser: src/main/java/com/datastax/loader/parser/DoubleParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/DoubleParser.java

FloatParser: src/main/java/com/datastax/loader/parser/FloatParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/FloatParser.java

InetAddressParser: src/main/java/com/datastax/loader/parser/InetAddressParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/InetAddressParser.java

IntegerParser: src/main/java/com/datastax/loader/parser/IntegerParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/IntegerParser.java

LongParser: src/main/java/com/datastax/loader/parser/LongParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/LongParser.java

NumberParser: src/main/java/com/datastax/loader/parser/NumberParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/NumberParser.java

StringParser: src/main/java/com/datastax/loader/parser/StringParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/StringParser.java

UUIDParser: src/main/java/com/datastax/loader/parser/UUIDParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/UUIDParser.java

DelimParser: src/main/java/com/datastax/loader/parser/DelimParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/DelimParser.java

CqlDelimParser: src/main/java/com/datastax/loader/parser/CqlDelimParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/parser/CqlDelimParser.java

CqlDelimLoad: src/main/java/com/datastax/loader/CqlDelimLoad.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" src/main/java/com/datastax/loader/CqlDelimLoad.java

jar: cassandra-loader.jar

explode: dirs
	unzip -oq lib/cassandra-java-driver/cassandra-driver-mapping-2.1.4.jar -d build
	unzip -oq lib/cassandra-java-driver/lib/metrics-core-3.0.2.jar -d build
	unzip -oq lib/cassandra-java-driver/lib/guava-14.0.1.jar -d build
	unzip -oq lib/cassandra-java-driver/lib/netty-3.9.0.Final.jar -d build
	unzip -oq lib/cassandra-java-driver/lib/lz4-1.2.0.jar -d build
	unzip -oq lib/cassandra-java-driver/lib/snappy-java-1.0.5.jar -d build
	unzip -oq lib/cassandra-java-driver/lib/slf4j-api-1.7.5.jar -d build
	unzip -oq lib/cassandra-java-driver/cassandra-driver-dse-2.1.4.jar -d build
	unzip -oq lib/cassandra-java-driver/cassandra-driver-core-2.1.4.jar -d build
	rm -rf build/META-INF

cassandra-loader.jar: compile explode
	jar cfe cassandra-loader.jar com.datastax.loader.CqlDelimLoad README.md lib/cassandra-java-driver -C build .

cassandra-loader: cassandra-loader.jar cassandra-loader.sh
	cat cassandra-loader.sh cassandra-loader.jar > cassandra-loader
	chmod 755 cassandra-loader

clean:
	- rm -rf build/* cassandra-loader.jar cassandra-loader

realclean: clean
	- rmdir build


