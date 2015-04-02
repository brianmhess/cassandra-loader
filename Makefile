dirs:
	- mkdir build

compile: dirs Parser NumberParser BigDecimalParser BigIntegerParser BooleanParser ByteBufferParser DateParser DoubleParser FloatParser InetAddressParser IntegerParser LongParser StringParser UUIDParser DelimParser CqlDelimParser CqlDelimLoad

Parser: parser/Parser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/Parser.java

BigDecimalParser: parser/BigDecimalParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/BigDecimalParser.java

BigIntegerParser: parser/BigIntegerParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/BigIntegerParser.java

BooleanParser: parser/BooleanParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/BooleanParser.java

ByteBufferParser: parser/ByteBufferParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/ByteBufferParser.java

DateParser: parser/DateParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/DateParser.java

DoubleParser: parser/DoubleParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/DoubleParser.java

FloatParser: parser/FloatParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/FloatParser.java

InetAddressParser: parser/InetAddressParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/InetAddressParser.java

IntegerParser: parser/IntegerParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/IntegerParser.java

LongParser: parser/LongParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/LongParser.java

NumberParser: parser/NumberParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/NumberParser.java

StringParser: parser/StringParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/StringParser.java

UUIDParser: parser/UUIDParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/UUIDParser.java

DelimParser: parser/DelimParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/DelimParser.java

CqlDelimParser: parser/CqlDelimParser.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" parser/CqlDelimParser.java

CqlDelimLoad: CqlDelimLoad.java
	javac -d build -cp "build:lib/cassandra-java-driver/*:lib/cassandra-java-driver/lib/*" CqlDelimLoad.java

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
	jar cfe cassandra-loader.jar hess.loader.CqlDelimLoad README.md lib/cassandra-java-driver -C build .

cassandra-loader: cassandra-loader.jar cassandra-loader.sh
	cat cassandra-loader.sh cassandra-loader.jar > cassandra-loader
	chmod 755 cassandra-loader

clean:
	- rm -rf build/* cassandra-loader.jar cassandra-loader

realclean: clean
	- rmdir build


