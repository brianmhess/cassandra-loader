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

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.StringTokenizer;
import java.lang.String;
import java.lang.System;
import java.lang.NumberFormatException;
import java.lang.IndexOutOfBoundsException;
import java.io.StringReader;
import java.io.IOException;
import java.text.ParseException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class DelimParser {
    private List<Parser> parsers;
    private int parsersSize;
    private List<Object> elements;
    private String delimiter;
    private String nullString;
    private char delim;
    private char quote;
    private char escape;

    public static String DEFAULT_DELIMITER = ",";
    public static String DEFAULT_NULLSTRING = "";

    public DelimParser() {
	this(DEFAULT_DELIMITER);
    }

    public DelimParser(String inDelimiter) {
	this(inDelimiter, DEFAULT_NULLSTRING);
    }

    public DelimParser(String inDelimiter, String inNullString) {
	parsers = new ArrayList<Parser>();
	elements = new ArrayList<Object>();
	parsersSize = parsers.size();
	if (null == inDelimiter)
	    delimiter = DEFAULT_DELIMITER;
	else 
	    delimiter = inDelimiter;
	if (null == inNullString)
	    nullString = DEFAULT_NULLSTRING;
	else
	    nullString = inNullString;
	delim = ("\\t".equals(delimiter)) ?  '\t' : delimiter.charAt(0);
	quote = '\"';
	escape = '\\';
    }
    
    // Adds a parser to the list
    public void add(Parser p) {
	parsers.add(p);
	parsersSize = parsers.size();
    }

    // Adds a collection of parsers to the list
    public void add(Collection<Parser> pl) {
	parsers.addAll(pl);
	parsersSize = parsers.size();
    }

    // This is where we apply rules like quoting, NULL, etc
    private String prepareToParse(String toparse) {
	String trimmedToParse = toparse.trim();
	if (trimmedToParse.startsWith("\"") && trimmedToParse.endsWith("\""))
	    trimmedToParse = trimmedToParse.substring(1, trimmedToParse.length() - 1);
	if (trimmedToParse.equals(nullString))
	    return null;
	return trimmedToParse;
    }

    public List<Object> parse(String line) {
	return parseComplex(line);
    }

    public List<Object> parseComplex(String line) {
	elements.clear();
	IndexedLine sr = new IndexedLine(line);
	for (int i = 0; i < parsersSize; i++) {
	    try {
		elements.add(parsers.get(i).parse(sr, nullString, delim, escape,
						  quote, (parsersSize-1 == i)));
	    }
	    catch (NumberFormatException e) {
		System.err.println(String.format("Invalid number in input number %d: %s", i, e.getMessage()));
		return null;
	    }
	    catch (ParseException pe) {
		System.err.println(String.format("Invalid format in input %d: %s", i, pe.getMessage()));
		return null;
	    }
	    catch (IOException e) {
		System.err.println(String.format("Invalid number of fields - ran out of string: %s", i, e.getMessage()));
		return null;
	    }
	}
	return elements;
    }

    // returns an array of Objects - to be used in PreparedStatement.bind()
    public Object[] getElements() {
	return elements.toArray();
    }

    public String format(Row row) throws IndexOutOfBoundsException, InvalidTypeException {
	String retVal = parsers.get(0).format(row, 0);
	for (int i = 1; i < parsersSize; i++) {
	    retVal = retVal + delimiter + parsers.get(i).format(row, i);
	}
	return retVal;
    }

}
