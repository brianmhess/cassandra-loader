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
package com.datastax.loader.parser;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.String;
import java.text.ParseException;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class CqlDelimParser {
    private Map<String, Parser> pmap;
    private List<SchemaBits> sbl;
    private String keyspace;
    private String tablename;
    private DelimParser delimParser;

    public CqlDelimParser(String inCqlSchema) throws ParseException {
	// Must supply a CQL schema
	this(inCqlSchema, null, null, false);
    }

    public CqlDelimParser(String inCqlSchema, String inDelimiter, String inNullString,
			  boolean delimiterInQuotes) throws ParseException {
	// Optionally provide things for the DelimParser - delmiter, null string
	this(inCqlSchema, inDelimiter, inNullString, delimiterInQuotes, null, null, null);
    }	

    public CqlDelimParser(String inCqlSchema, String inDelimiter, String inNullString,
			  boolean delimiterInQuotes, String inDateFormatString, 
			  BooleanParser.BoolStyle inBoolStyle, Locale inLocale) throws ParseException {
	// Optionally provide things for the line parser - date format, boolean format, locale
	initPmap(inDateFormatString, inBoolStyle, inLocale);
	processCqlSchema(inCqlSchema);
	createDelimParser(inDelimiter, inNullString, delimiterInQuotes);
    }	

    // used internally to store schema information
    private class SchemaBits {
	public String name;
	public String datatype;
	public Parser parser;
    }

    // intialize the Parsers and the parser map
    private void initPmap(String dateFormatString, BooleanParser.BoolStyle inBoolStyle, 
			  Locale inLocale) {
	pmap = new HashMap<String, Parser>();
	Parser integerParser = new IntegerParser(inLocale);
	Parser longParser = new LongParser(inLocale);
	Parser floatParser = new FloatParser(inLocale);
	Parser doubleParser = new DoubleParser(inLocale);
	Parser stringParser = new StringParser();
	Parser booleanParser = new BooleanParser(inBoolStyle);
	Parser uuidParser = new UUIDParser();
	Parser bigDecimalParser = new BigDecimalParser();
	Parser bigIntegerParser = new BigIntegerParser();
	Parser byteBufferParser = new ByteBufferParser();
	Parser inetAddressParser = new InetAddressParser();
	Parser dateParser = new DateParser(dateFormatString);

	pmap.put("ASCII", stringParser);
	pmap.put("BIGINT", longParser);
	pmap.put("BLOB", byteBufferParser);
	pmap.put("BOOLEAN", booleanParser);
	pmap.put("COUNTER", longParser);
	pmap.put("DECIMAL", bigDecimalParser);
	pmap.put("DOUBLE", doubleParser);
	pmap.put("FLOAT", floatParser);
	pmap.put("INET", inetAddressParser);
	pmap.put("INT", integerParser);
	pmap.put("TEXT", stringParser);
	pmap.put("TIMESTAMP", dateParser);
	pmap.put("TIMEUUID", uuidParser);
	pmap.put("UUID", uuidParser);
	pmap.put("VARCHAR", stringParser);
	pmap.put("VARINT", bigIntegerParser);
    }

    // Validate the CQL schema, extract the keyspace and tablename, and process the rest of the schema
    private void processCqlSchema(String cqlSchema) throws ParseException {
	String kstnRegex = "^\\s*(\\w+)\\.(\\w+)\\s*[\\(]\\s*(\\w+\\s+\\w+\\s*(,\\s*\\w+\\s+\\w+\\s*)*)[\\)]\\s*$";
	Pattern p = Pattern.compile(kstnRegex);
	Matcher m = p.matcher(cqlSchema);
	if (!m.find()) {
	    throw new ParseException("Badly formatted schema", 0);
	}
	keyspace = m.group(1);
	tablename = m.group(2);
	String schemaString = m.group(3);
	sbl = parseSchema(schemaString);
    }

    // Validate the schema and extract name and data type information.  Builds up the list of parsers
    private List<SchemaBits> parseSchema(String in) throws ParseException {
	String schemaRegex = ",*\\s*(\\w+)\\s+(\\w+)\\s*";
	Pattern p = Pattern.compile(schemaRegex);
	Matcher m = p.matcher(in);

	if (!m.find()) {
	    throw new ParseException("Badly formatted CQL schema", 0);
	}

	List<SchemaBits> sbl = new ArrayList<SchemaBits>();
	int i = 1;
	do {
	    SchemaBits sb = new SchemaBits();
	    sb.name = m.group(1);
	    sb.datatype = m.group(2).toUpperCase();
	    sb.parser = pmap.get(sb.datatype);
	    if (null == sb.parser) {
		throw new ParseException("Column data type not recognized (" + sb.datatype + ")", i);
	    }
	    sbl.add(sb);
	    i++;
	} while (m.find());
	return sbl;
    }

    // Creates the DelimParser that will parse the line
    private void createDelimParser(String delimiter, String nullString, boolean delimiterInQuotes) {
	delimParser = new DelimParser(delimiter, nullString, delimiterInQuotes);
	for (int i = 0; i < sbl.size(); i++)
	    delimParser.add(sbl.get(i).parser);
    }

    // Convenience method to return the INSERT statement for a PreparedStatement.
    public String generateInsert() {
	String insert = "INSERT INTO " + keyspace + "." + tablename + "(" + sbl.get(0).name;
	String qmarks = "?";
	for (int i = 1; i < sbl.size(); i++) {
	    insert = insert + ", " + sbl.get(i).name;
	    qmarks = qmarks + ", ?";
	}
	insert = insert + ") VALUES (" + qmarks + ")";
	return insert;
    }

    // Pass through to parse the line - the DelimParser we created will be used.
    public List<Object> parse(String line) {
	return delimParser.parse(line);
    }
}

