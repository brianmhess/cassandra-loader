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

import com.datastax.loader.parser.Parser;
import com.datastax.loader.parser.DelimParser;
import com.datastax.loader.parser.IntegerParser;
import com.datastax.loader.parser.LongParser;
import com.datastax.loader.parser.FloatParser;
import com.datastax.loader.parser.DoubleParser;
import com.datastax.loader.parser.StringParser;
import com.datastax.loader.parser.BooleanParser;
import com.datastax.loader.parser.UUIDParser;
import com.datastax.loader.parser.BigDecimalParser;
import com.datastax.loader.parser.BigIntegerParser;
import com.datastax.loader.parser.ByteBufferParser;
import com.datastax.loader.parser.InetAddressParser;
import com.datastax.loader.parser.DateParser;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.String;
import java.text.ParseException;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.lang.IndexOutOfBoundsException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class CqlDelimParser {
    private Map<String, Parser> pmap;
    private List<SchemaBits> sbl;
    private String keyspace;
    private String tablename;
    private DelimParser delimParser;

    public CqlDelimParser(String inCqlSchema, Session session) throws ParseException {
	// Must supply a CQL schema
	this(inCqlSchema, null, null, false, session);
    }

    public CqlDelimParser(String inCqlSchema, String inDelimiter, String inNullString,
			  boolean delimiterInQuotes, Session session) throws ParseException {
	// Optionally provide things for the DelimParser - delmiter, null string
	this(inCqlSchema, inDelimiter, inNullString, delimiterInQuotes, null, null, null, session);
    }	

    public CqlDelimParser(String inCqlSchema, String inDelimiter, String inNullString,
			  boolean delimiterInQuotes, String inDateFormatString, 
			  BooleanParser.BoolStyle inBoolStyle, Locale inLocale, Session session) throws ParseException {
	// Optionally provide things for the line parser - date format, boolean format, locale
	initPmap(inDateFormatString, inBoolStyle, inLocale);
	processCqlSchema(inCqlSchema, session);
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
    private void processCqlSchema(String cqlSchema, Session session) throws ParseException {
	String kstnRegex = "^\\s*(\\w+)\\.(\\w+)\\s*[\\(]\\s*(\\w+\\s*(,\\s*\\w+\\s*)*)[\\)]\\s*$";
	Pattern p = Pattern.compile(kstnRegex);
	Matcher m = p.matcher(cqlSchema);
	if (!m.find()) {
	    throw new ParseException("Badly formatted schema", 0);
	}
	keyspace = m.group(1);
	tablename = m.group(2);
	String schemaString = m.group(3);
	sbl = schemaBits(schemaString, session);
    }

    private List<SchemaBits> schemaBits(String in, Session session) throws ParseException {
	String query = "SELECT " + in + " FROM " + keyspace + "." + tablename + " LIMIT 1";
	ColumnDefinitions cd = session.execute(query).getColumnDefinitions();
	String[] inList = in.split(",");
	List<SchemaBits> sbl = new ArrayList<SchemaBits>();
	for (int i = 0; i < inList.length; i++) {
	    String col = inList[i].trim();
	    SchemaBits sb = new SchemaBits();
	    sb.name = col;
	    sb.datatype = cd.getType(col).getName().toString().toUpperCase();
	    sb.parser = pmap.get(sb.datatype);
	    if (null == sb.parser) {
		throw new ParseException("Column data type not recognized (" + sb.datatype + ")", i);
	    }
	    sbl.add(sb);
	}
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

    public String generateSelect() {
	String select = "SELECT " + sbl.get(0).name;
	for (int i = 1; i < sbl.size(); i++) {
	    select = select + ", " + sbl.get(i).name;
	}
	select += " FROM " + keyspace + "." + tablename;
	return select;
    }
    
    public String getKeyspace() {
	return keyspace;
    }

    public String getTable() {
	return tablename;
    }

    // Pass through to parse the line - the DelimParser we created will be used.
    public List<Object> parse(String line) {
	return delimParser.parse(line);
    }

    public String format(Row row) throws IndexOutOfBoundsException, InvalidTypeException {
	return delimParser.format(row);
    }
}

